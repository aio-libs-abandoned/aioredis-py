from __future__ import annotations

import asyncio
import enum
import errno
import inspect
import os
import socket
import ssl
import threading
import warnings
from collections import deque
from distutils.version import StrictVersion
from itertools import chain
from types import MappingProxyType
from typing import (
    Callable,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)
from urllib.parse import ParseResult, parse_qs, unquote, urlparse

import async_timeout

from .compat import Protocol, TypedDict
from .exceptions import (
    AuthenticationError,
    AuthenticationWrongNumberOfArgsError,
    BusyLoadingError,
    ChildDeadlockedError,
    ConnectionError,
    DataError,
    ExecAbortError,
    InvalidResponse,
    ModuleError,
    NoPermissionError,
    NoScriptError,
    ProtocolError,
    ReadOnlyError,
    RedisError,
    ReplyError,
    ResponseError,
    TimeoutError,
)
from .parser import PythonReader
from .utils import str_if_bytes

NONBLOCKING_EXCEPTION_ERROR_NUMBERS = {
    BlockingIOError: errno.EWOULDBLOCK,
    ssl.SSLWantReadError: 2,
    ssl.SSLWantWriteError: 2,
    ssl.SSLError: 2,
}

NONBLOCKING_EXCEPTIONS = tuple(NONBLOCKING_EXCEPTION_ERROR_NUMBERS.keys())

try:
    import hiredis

except (ImportError, ModuleNotFoundError):
    HIREDIS_AVAILABLE = False
else:
    HIREDIS_AVAILABLE = True
    hiredis_version = StrictVersion(hiredis.__version__)
    if hiredis_version < StrictVersion("1.0.0"):
        warnings.warn(
            "aioredis supports hiredis @ 1.0.0 or higher. "
            f"You have hiredis @ {hiredis.__version__}. "
            "Pure-python parser will be used instead."
        )
        HIREDIS_AVAILABLE = False

DefaultReader: type[PythonReader] | type[hiredis.Reader]
if HIREDIS_AVAILABLE:
    DefaultReader = hiredis.Reader
else:
    DefaultReader = PythonReader

SYM_STAR = b"*"
SYM_DOLLAR = b"$"
SYM_CRLF = b"\r\n"
SYM_LF = b"\n"
SYM_EMPTY = b""

SERVER_CLOSED_CONNECTION_ERROR = "Connection closed by server."


class _Sentinel(enum.Enum):
    sentinel = object()


SENTINEL = _Sentinel.sentinel
MODULE_LOAD_ERROR = "Error loading the extension. Please check the server logs."
NO_SUCH_MODULE_ERROR = "Error unloading module: no such module with that name"
MODULE_UNLOAD_NOT_POSSIBLE_ERROR = "Error unloading module: operation not possible."
MODULE_EXPORTS_DATA_TYPES_ERROR = (
    "Error unloading module: the module "
    "exports one or more module-side data "
    "types, can't unload"
)

EncodedT = Union[bytes, bytearray, memoryview]
DecodedT = Union[str, int, float]
EncodableT = Union[EncodedT, DecodedT]


class _HiredisReaderArgs(TypedDict, total=False):
    protocolError: Callable[[str], Exception]
    replyError: Callable[[str], Exception]
    encoding: str | None
    errors: str | None


class Encoder:
    """Encode strings to bytes-like and decode bytes-like to strings"""

    __slots__ = "encoding", "encoding_errors", "decode_responses"

    def __init__(self, encoding: str, encoding_errors: str, decode_responses: bool):
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.decode_responses = decode_responses

    def encode(self, value: EncodableT) -> EncodedT:
        """Return a bytestring or bytes-like representation of the value"""
        if isinstance(value, (bytes, memoryview)):
            return value
        if isinstance(value, bool):
            # special case bool since it is a subclass of int
            raise DataError(
                "Invalid input of type: 'bool'. "
                "Convert to a bytes, string, int or float first."
            )
        if isinstance(value, (int, float)):
            return repr(value).encode()
        if not isinstance(value, str):
            # a value we don't know how to deal with. throw an error
            typename = value.__class__.__name__  # type: ignore[unreachable]
            raise DataError(
                f"Invalid input of type: {typename!r}. "
                "Convert to a bytes, string, int or float first."
            )
        return value.encode(self.encoding, self.encoding_errors)

    def decode(self, value: EncodableT, force=False) -> EncodableT:
        """Return a unicode string from the bytes-like representation"""
        if self.decode_responses or force:
            if isinstance(value, memoryview):
                return value.tobytes().decode(self.encoding, self.encoding_errors)
            if isinstance(value, bytes):
                return value.decode(self.encoding, self.encoding_errors)
        return value


ExceptionMappingT = Mapping[str, Union[Type[Exception], Mapping[str, Type[Exception]]]]


@enum.unique
class _State(enum.IntEnum):
    not_connected = enum.auto()
    connected = enum.auto()
    error = enum.auto()


class RedisProtocol(asyncio.Protocol):
    """A Redis-specific :py:class:`asyncio.Protocol`."""

    def __init__(
        self,
        *,
        reader: DefaultReader | None = None,
        encoding: str | None = None,
        encoding_errors: str | None = None,
        socket_keepalive: bool = False,
        socket_keepalive_options: Mapping[int, int | bytes] | None = None,
    ):
        self._state = _State.not_connected
        self._resp_queue: asyncio.Queue = asyncio.Queue()
        self._transport: asyncio.Transport | None = None
        self._parser: DefaultReader = reader or DefaultReader(
            ProtocolError,
            ReplyError,
            encoding=encoding,
            errors=encoding_errors,
        )
        self._responder_task = None
        self._exc: BaseException | None = None
        self._conn_waiter: asyncio.Event = asyncio.Event()
        self._socket_keepalive: bool = socket_keepalive
        self._socket_keepalive_options: Mapping[int, int | bytes] = (
            socket_keepalive_options or {}
        )

    def connection_made(self, transport: asyncio.Transport) -> None:
        self._transport = transport
        sock = transport.get_extra_info("socket")
        if sock is not None:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            # TCP_KEEPALIVE
            if self._socket_keepalive:
                try:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    for k, v in self._socket_keepalive_options.items():
                        sock.setsockopt(socket.SOL_TCP, k, v)
                except (OSError, TypeError):
                    # `socket_keepalive_options` might contain invalid options
                    # causing an error. Do not leave the connection open.
                    sock.close()
                    raise

        self._state = _State.connected
        self._conn_waiter.set()

    @property
    def connected(self) -> bool:
        return self._state == _State.connected

    async def wait_connected(self) -> None:
        """Wait to access the connection until `connection_made` is complete."""
        await self._conn_waiter.wait()

    def send_command(self, cmd: bytes) -> asyncio.Future:
        fut = asyncio.get_event_loop().create_future()

        if self._state == _State.connected:
            # It's possible that connection was dropped and connection_callback was not
            # called yet, to stop spamming errors, avoid writing to broken pipe
            # Both _UnixWritePipeTransport and _SelectorSocketTransport that we
            # expect to see here have this attribute
            if not self._transport.is_closing():
                self._transport.write(cmd)
            self._resp_queue.put_nowait(fut)

        elif self._state == _State.not_connected:
            fut.set_exception(
                ConnectionError(f"Lost connection while sending command: {cmd!r}")
            )

        elif self._state == _State.error:
            exc = self._exc
            if exc is None:
                exc = ConnectionError(
                    f"Got an unknown error while sending command: {cmd}."
                )
            fut.set_exception(exc)

        return fut

    def data_received(self, data):
        if self._state != _State.connected:
            return

        self._parser.feed(data)
        res = self._parser.gets()

        while res is not False:
            try:
                fut = self._resp_queue.get_nowait()
            except asyncio.QueueEmpty:
                # # Extra unexpected data received from connection
                # # e.g. connected to non-redis service
                # self._set_exception(
                #     InvalidResponse(
                #         "Got additional data on the stream. "
                #         "Are you connected to a supported Redis instance?"
                #     )
                # )
                pass

            else:
                if isinstance(res, RedisError):
                    err = self.parse_error(str(res))
                    if isinstance(err, ConnectionError):
                        # If we got a connection error,
                        #   we need to shut down the Protocol
                        self._set_exception(err)
                    fut.set_exception(err)
                else:
                    fut.set_result(res)

            res = self._parser.gets()

    def _set_exception(self, exc):
        self._exc = exc
        self._state = _State.error

    def connection_lost(self, exc):
        if exc is not None:
            self._set_exception(exc)

        elif self._state == _State.connected:
            exc = ConnectionError("Lost connection to server.")
            self._state = _State.not_connected
            self._set_exception(exc)

        while not self._resp_queue.empty():
            fut = self._resp_queue.get_nowait()
            fut.set_result(self._exc)

    def can_read(self):
        return self._parser.has_data()

    def parse_error(self, response: str) -> ResponseError:
        """Parse an error response"""
        error_code = response.split(" ")[0]
        if error_code in self.EXCEPTION_CLASSES:
            response = response[len(error_code) + 1 :]
            exception_class_or_dict = self.EXCEPTION_CLASSES[error_code]
            if isinstance(exception_class_or_dict, dict):
                exception_class = exception_class_or_dict.get(response, ResponseError)
            else:
                exception_class = exception_class_or_dict
            return exception_class(response)
        return ResponseError(response)

    EXCEPTION_CLASSES: ExceptionMappingT = {
        "ERR": {
            "max number of clients reached": ConnectionError,
            "Client sent AUTH, but no password is set": AuthenticationError,
            "invalid password": AuthenticationError,
            # some Redis server versions report invalid command syntax
            # in lowercase
            "wrong number of arguments for 'auth' command": AuthenticationWrongNumberOfArgsError,
            # some Redis server versions report invalid command syntax
            # in uppercase
            "wrong number of arguments for 'AUTH' command": AuthenticationWrongNumberOfArgsError,
            MODULE_LOAD_ERROR: ModuleError,
            MODULE_EXPORTS_DATA_TYPES_ERROR: ModuleError,
            NO_SUCH_MODULE_ERROR: ModuleError,
            MODULE_UNLOAD_NOT_POSSIBLE_ERROR: ModuleError,
        },
        "EXECABORT": ExecAbortError,
        "LOADING": BusyLoadingError,
        "NOSCRIPT": NoScriptError,
        "READONLY": ReadOnlyError,
        "NOAUTH": AuthenticationError,
        "NOPERM": NoPermissionError,
    }


class ConnectCallbackProtocol(Protocol):
    def __call__(self, connection: Connection):
        ...


class AsyncConnectCallbackProtocol(Protocol):
    async def __call__(self, connection: "Connection"):
        ...


ConnectCallbackT = Union[ConnectCallbackProtocol, AsyncConnectCallbackProtocol]


class Connection:
    """Manages TCP communication to and from a Redis server"""

    __slots__ = (
        "pid",
        "host",
        "port",
        "db",
        "username",
        "client_name",
        "password",
        "socket_timeout",
        "socket_connect_timeout",
        "socket_keepalive",
        "socket_keepalive_options",
        "socket_type",
        "retry_on_timeout",
        "health_check_interval",
        "next_health_check",
        "last_active_at",
        "encoder",
        "ssl_context",
        "_protocol",
        "_connect_callbacks",
        "_buffer_cutoff",
        "_lock",
        "_conn",
        "__dict__",
    )

    def __init__(
        self,
        *,
        host: str = "localhost",
        port: str | int = 6379,
        db: str | int = 0,
        password: str | None = None,
        socket_timeout: float | None = None,
        socket_connect_timeout: float | None = None,
        socket_keepalive: bool = False,
        socket_keepalive_options: Mapping[int, int | bytes] | None = None,
        socket_type: int = 0,
        retry_on_timeout: bool = False,
        encoding: str = "utf-8",
        encoding_errors: str = "strict",
        decode_responses: bool = False,
        parser_class: type[DefaultReader] | None = None,
        socket_read_size: int = 65536,
        health_check_interval: float = 0,
        client_name: str | None = None,
        username: str | None = None,
        encoder_class: type[Encoder] = Encoder,
    ):
        self.pid = os.getpid()
        self.host = host
        self.port = int(port)
        self.db = db
        self.username = username
        self.client_name = client_name
        self.password = password
        self.socket_timeout = socket_timeout
        self.socket_connect_timeout = socket_connect_timeout or socket_timeout or None
        self.socket_keepalive = socket_keepalive
        self.socket_keepalive_options = socket_keepalive_options or {}
        self.socket_type = socket_type
        self.retry_on_timeout = retry_on_timeout
        self.health_check_interval = health_check_interval
        self.next_health_check: float = -1
        self.ssl_context: RedisSSLContext | None = None
        self.encoder = encoder_class(encoding, encoding_errors, decode_responses)
        self._connect_callbacks: list[ConnectCallbackT] = []
        self._buffer_cutoff = 6000
        self._lock = asyncio.Lock()
        self._conn: asyncio.Transport | None = None
        reader = None
        encoding = None if decode_responses is False else encoding
        if parser_class:
            reader = parser_class(
                ProtocolError, ReplyError, encoding=encoding, errors=encoding_errors
            )
        self._protocol = RedisProtocol(
            reader=reader,
            encoding=encoding,
            encoding_errors=encoding_errors,
            socket_keepalive=socket_keepalive,
            socket_keepalive_options=socket_keepalive_options,
        )

    def __repr__(self):
        repr_args = ",".join((f"{k}={v}" for k, v in self.repr_pieces()))
        return f"{self.__class__.__name__}<{repr_args}>"

    def repr_pieces(self):
        pieces = [("host", self.host), ("port", self.port), ("db", self.db)]
        if self.client_name:
            pieces.append(("client_name", self.client_name))
        return pieces

    def __del__(self):
        try:
            if self.is_connected:
                warnings.warn(
                    f"Unclosed connection {self!r}",
                    ResourceWarning,
                    source=self,
                )
                context = {"connection": self, "message": "Unclosed connection"}
                asyncio.get_event_loop().call_exception_handler(context)
        except (AttributeError, RuntimeError):
            pass

    @property
    def is_connected(self):
        return self._conn is not None and not self._conn.is_closing()

    def register_connect_callback(self, callback):
        self._connect_callbacks.append(callback)

    def clear_connect_callbacks(self):
        self._connect_callbacks = []

    async def connect(self):
        """Connects to the Redis server if not already connected"""
        if self.is_connected:
            return
        try:
            await self._connect()
        except asyncio.CancelledError:
            raise
        except (socket.timeout, asyncio.TimeoutError):
            raise TimeoutError("Timeout connecting to server")
        except OSError as e:
            raise ConnectionError(self._error_message(e))
        except Exception as exc:
            raise ConnectionError(exc) from exc

        try:
            await self.on_connect()
        except RedisError:
            # clean up after any error in on_connect
            await self.disconnect()
            raise

        # run any user callbacks. right now the only internal callback
        # is for pubsub channel/pattern resubscription
        for callback in self._connect_callbacks:
            task = callback(self)
            if task and inspect.isawaitable(task):
                await task

    async def _connect(self):
        """Create a TCP socket connection"""
        async with async_timeout.timeout(self.socket_connect_timeout):
            self._conn, _ = await asyncio.get_event_loop().create_connection(
                lambda: self._protocol,
                host=self.host,
                port=self.port,
                ssl=self.ssl_context.get() if self.ssl_context else None,
            )
            # await self._protocol.wait_connected()

    def _error_message(self, exception):
        # args for socket.error can either be (errno, "message")
        # or just "message"
        if len(exception.args) == 1:
            return f"Error connecting to {self.host}:{self.port}. {exception.args[0]}."
        else:
            return (
                f"Error {exception.args[0]} connecting to {self.host}:{self.port}. "
                f"{exception.args[0]}."
            )

    async def on_connect(self):
        """Initialize the connection, authenticate and select a database"""
        # self._parser.on_connect(self)

        # if username and/or password are set, authenticate
        if self.username or self.password:
            auth_args: tuple[str] | tuple[str, str]
            if self.username:
                auth_args = (self.username, self.password or "")
            else:
                # Mypy bug: https://github.com/python/mypy/issues/10944
                auth_args = (self.password or "",)
            # avoid checking health here -- PING will fail if we try
            # to check the health prior to the AUTH

            try:
                auth_response = await self.send_command(
                    "AUTH", *auth_args, check_health=False
                )
            except AuthenticationWrongNumberOfArgsError:
                # a username and password were specified but the Redis
                # server seems to be < 6.0.0 which expects a single password
                # arg. retry auth with just the password.
                # https://github.com/andymccurdy/redis-py/issues/1274
                auth_response = await self.send_command(
                    "AUTH", self.password, check_health=False
                )

            if str_if_bytes(auth_response) != "OK":
                raise AuthenticationError("Invalid Username or Password")

        # if a client_name is given, set it
        if self.client_name:
            resp = await self.send_command("CLIENT", "SETNAME", self.client_name)
            if str_if_bytes(resp) != "OK":
                raise ConnectionError("Error setting client name")

        # if a database is specified, switch to it
        if self.db:
            dbresp = str_if_bytes(await self.send_command("SELECT", self.db))

            if dbresp != "OK":
                raise ConnectionError(f"Invalid Database: {dbresp}")

    async def disconnect(self):
        """Disconnects from the Redis server"""
        try:
            async with async_timeout.timeout(self.socket_connect_timeout):
                # self._parser.on_disconnect()
                if not self.is_connected:
                    return
                try:
                    self._conn.close()
                    # py3.6 doesn't have this method
                    if hasattr(self._conn, "wait_closed"):
                        await self._conn.wait_closed()  # type: ignore[union-attr]
                    self._conn = None
                except OSError:
                    pass
        except asyncio.TimeoutError:
            raise TimeoutError(
                f"Timed out closing connection after {self.socket_connect_timeout}"
            ) from None

    async def check_health(self):
        """Check the health of the connection with a PING/PONG"""
        if (
            self.health_check_interval
            and asyncio.get_event_loop().time() > self.next_health_check
        ):
            try:
                await self.send_command("PING", check_health=False)
                if str_if_bytes(await self.read_response()) != "PONG":
                    raise ConnectionError("Bad response from PING health check")
            except (ConnectionError, TimeoutError) as err:
                await self.disconnect()
                try:
                    await self.send_command("PING", check_health=False)
                    if str_if_bytes(await self.read_response()) != "PONG":
                        raise ConnectionError(
                            "Bad response from PING health check"
                        ) from None
                except BaseException as err2:
                    raise err2 from err

    def _send_packed_command(self, command: bytearray):
        return self._protocol.send_command(command)

    async def send_packed_command(
        self,
        command: str | bytes | bytearray,
        check_health: bool = True,
    ):
        """Send an already packed command to the Redis server"""
        # guard against health check recursion
        # if check_health:
        #     await self.check_health()
        try:
            if isinstance(command, str):
                command = command.encode()
            if isinstance(command, bytes):
                command = bytearray(command)
            return await self._send_packed_command(command)
        except asyncio.TimeoutError:
            await self.disconnect()
            raise TimeoutError("Timeout writing to socket") from None
        except OSError as e:
            await self.disconnect()
            if len(e.args) == 1:
                err_no, errmsg = "UNKNOWN", e.args[0]
            else:
                err_no = e.args[0]
                errmsg = e.args[1]
            raise ConnectionError(
                f"Error {err_no} while writing to socket. {errmsg}."
            ) from e
        except BaseException:
            await self.disconnect()
            raise

    async def send_command(self, *args, **kwargs):
        """Pack and send a command to the Redis server"""
        if not self.is_connected:
            await self.connect()
        return await self.send_packed_command(
            self.pack_command(*args), check_health=kwargs.get("check_health", True)
        )

    async def can_read(self, timeout: float = 0):
        """Poll the socket to see if there's data that can be read."""
        if not self.is_connected:
            await self.connect()
        return self._protocol.can_read()

    async def read_response(self):
        """Read the response from a previously sent command"""
        raise NotImplementedError("should not be called")

    def pack_command(self, *args: EncodableT, buff: bytearray = None) -> bytearray:
        """Pack a series of arguments into the Redis protocol"""
        buff = buff or bytearray()
        cmd = args[0]
        if (
            isinstance(cmd, str)
            and " " in cmd
            or isinstance(cmd, bytes)
            and b" " in cmd
        ):
            args = (*cmd.split(), *args[1:])
        buff.extend(b"*%d\r\n" % len(args))

        _convs = self._converters
        _extend = buff.extend
        for arg in args:
            barg = _convs[arg.__class__](arg)
            if b" " in barg:
                for _barg in barg.split():
                    _extend(b"$%d\r\n%s\r\n" % (len(barg), barg))
                continue
            _extend(b"$%d\r\n%s\r\n" % (len(barg), barg))

        return buff

    _converters = {
        bytes: lambda val: val,
        bytearray: lambda val: val,
        memoryview: lambda val: val,
        str: lambda val: val.encode(),
        int: lambda val: b"%d" % val,
        float: lambda val: b"%r" % val,
    }

    def pack_commands(self, commands: Iterable[Iterable[EncodableT]]) -> bytearray:
        """Pack multiple commands into the Redis protocol"""
        output: bytearray = bytearray()
        for cmd in commands:
            self.pack_command(*cmd, buff=output)
        return output


class SSLConnection(Connection):
    def __init__(
        self,
        ssl_keyfile: str | None = None,
        ssl_certfile: str | None = None,
        ssl_cert_reqs: str = "required",
        ssl_ca_certs: str | None = None,
        ssl_check_hostname: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.ssl_context: RedisSSLContext = RedisSSLContext(
            keyfile=ssl_keyfile,
            certfile=ssl_certfile,
            cert_reqs=ssl_cert_reqs,
            ca_certs=ssl_ca_certs,
            check_hostname=ssl_check_hostname,
        )

    @property
    def keyfile(self):
        return self.ssl_context.keyfile

    @property
    def certfile(self):
        return self.ssl_context.certfile

    @property
    def cert_reqs(self):
        return self.ssl_context.cert_reqs

    @property
    def ca_certs(self):
        return self.ssl_context.ca_certs

    @property
    def check_hostname(self):
        return self.ssl_context.check_hostname


class RedisSSLContext:
    __slots__ = (
        "keyfile",
        "certfile",
        "cert_reqs",
        "ca_certs",
        "context",
        "check_hostname",
    )

    def __init__(
        self,
        keyfile: str | None = None,
        certfile: str | None = None,
        cert_reqs: str | None = None,
        ca_certs: str | None = None,
        check_hostname: bool = False,
    ):
        self.keyfile = keyfile
        self.certfile = certfile
        if cert_reqs is None:
            self.cert_reqs = ssl.CERT_NONE
        elif isinstance(cert_reqs, str):
            CERT_REQS = {
                "none": ssl.CERT_NONE,
                "optional": ssl.CERT_OPTIONAL,
                "required": ssl.CERT_REQUIRED,
            }
            if cert_reqs not in CERT_REQS:
                raise RedisError(
                    f"Invalid SSL Certificate Requirements Flag: {cert_reqs}"
                )
            self.cert_reqs = CERT_REQS[cert_reqs]
        self.ca_certs = ca_certs
        self.check_hostname = check_hostname
        self.context: ssl.SSLContext | None = None

    def get(self) -> ssl.SSLContext:
        if not self.context:
            context = ssl.create_default_context()
            context.check_hostname = self.check_hostname
            context.verify_mode = self.cert_reqs
            if self.certfile and self.keyfile:
                context.load_cert_chain(certfile=self.certfile, keyfile=self.keyfile)
            if self.ca_certs:
                context.load_verify_locations(self.ca_certs)
            self.context = context
        return self.context


class UnixDomainSocketConnection(Connection):  # lgtm [py/missing-call-to-init]
    def __init__(
        self,
        *,
        path: str = "",
        db: str | int = 0,
        username: str | None = None,
        password: str | None = None,
        socket_timeout: float | None = None,
        socket_connect_timeout: float | None = None,
        encoding: str = "utf-8",
        encoding_errors: str = "strict",
        decode_responses: bool = False,
        retry_on_timeout: bool = False,
        parser_class: type[DefaultReader] = DefaultReader,
        socket_read_size: int = 65536,
        health_check_interval: float = 0.0,
        client_name=None,
    ):
        self.pid = os.getpid()
        self.path = path
        self.db = db
        self.username = username
        self.client_name = client_name
        self.password = password
        self.socket_timeout = socket_timeout
        self.socket_connect_timeout = socket_connect_timeout or socket_timeout or None
        self.retry_on_timeout = retry_on_timeout
        self.health_check_interval = health_check_interval
        self.next_health_check = -1
        self.encoder = Encoder(encoding, encoding_errors, decode_responses)
        self._sock = None
        self._reader = None
        self._writer = None
        self._parser = parser_class(socket_read_size=socket_read_size)
        self._connect_callbacks = []
        self._buffer_cutoff = 6000
        self._lock = asyncio.Lock()

    def repr_pieces(self) -> Iterable[tuple[str, str | int]]:
        pieces = [
            ("path", self.path),
            ("db", self.db),
        ]
        if self.client_name:
            pieces.append(("client_name", self.client_name))
        return pieces

    async def _connect(self):
        async with async_timeout.timeout(self.socket_connect_timeout):
            reader, writer = await asyncio.open_unix_connection(path=self.path)
        self._reader = reader
        self._writer = writer
        await self.on_connect()

    def _error_message(self, exception):
        # args for socket.error can either be (errno, "message")
        # or just "message"
        if len(exception.args) == 1:
            return f"Error connecting to unix socket: {self.path}. {exception.args[0]}."
        else:
            return (
                f"Error {exception.args[0]} connecting to unix socket: "
                f"{self.path}. {exception.args[1]}."
            )


FALSE_STRINGS = ("0", "F", "FALSE", "N", "NO")


def to_bool(value) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, str) and value.upper() in FALSE_STRINGS:
        return False
    return bool(value)


URL_QUERY_ARGUMENT_PARSERS: Mapping[str, Callable[..., object]] = MappingProxyType(
    {
        "db": int,
        "socket_timeout": float,
        "socket_connect_timeout": float,
        "socket_keepalive": to_bool,
        "retry_on_timeout": to_bool,
        "max_connections": int,
        "health_check_interval": int,
        "ssl_check_hostname": to_bool,
    }
)


class ConnectKwargs(TypedDict, total=False):
    username: str
    password: str
    connection_class: type[Connection]
    host: str
    port: int
    db: int
    path: str


def parse_url(url: str) -> ConnectKwargs:
    parsed: ParseResult = urlparse(url)
    kwargs: ConnectKwargs = {}

    for name, value_list in parse_qs(parsed.query).items():
        if value_list and len(value_list) > 0:
            value = unquote(value_list[0])
            parser = URL_QUERY_ARGUMENT_PARSERS.get(name)
            if parser:
                try:
                    # We can't type this.
                    kwargs[name] = parser(value)  # type: ignore[misc]
                except (TypeError, ValueError):
                    raise ValueError(f"Invalid value for `{name}` in connection URL.")
            else:
                kwargs[name] = value  # type: ignore[misc]

    if parsed.username:
        kwargs["username"] = unquote(parsed.username)
    if parsed.password:
        kwargs["password"] = unquote(parsed.password)

    # We only support redis://, rediss:// and unix:// schemes.
    if parsed.scheme == "unix":
        if parsed.path:
            kwargs["path"] = unquote(parsed.path)
        kwargs["connection_class"] = UnixDomainSocketConnection

    elif parsed.scheme in ("redis", "rediss"):
        if parsed.hostname:
            kwargs["host"] = unquote(parsed.hostname)
        if parsed.port:
            kwargs["port"] = int(parsed.port)

        # If there's a path argument, use it as the db argument if a
        # querystring value wasn't specified
        if parsed.path and "db" not in kwargs:
            try:
                kwargs["db"] = int(unquote(parsed.path).replace("/", ""))
            except (AttributeError, ValueError):
                pass

        if parsed.scheme == "rediss":
            kwargs["connection_class"] = SSLConnection
    else:
        valid_schemes = "redis://, rediss://, unix://"
        raise ValueError(
            f"Redis URL must specify one of the following schemes ({valid_schemes})"
        )

    return kwargs


_CP = TypeVar("_CP", bound="ConnectionPool")


class ConnectionPool:
    """
    Create a connection pool. ``If max_connections`` is set, then this
    object raises :py:class:`~redis.ConnectionError` when the pool's
    limit is reached.

    By default, TCP connections are created unless ``connection_class``
    is specified. Use :py:class:`~redis.UnixDomainSocketConnection` for
    unix sockets.

    Any additional keyword arguments are passed to the constructor of
    ``connection_class``.
    """

    @classmethod
    def from_url(cls: type[_CP], url: str, **kwargs) -> _CP:
        """
        Return a connection pool configured from the given URL.

        For example::

            redis://[[username]:[password]]@localhost:6379/0
            rediss://[[username]:[password]]@localhost:6379/0
            unix://[[username]:[password]]@/path/to/socket.sock?db=0

        Three URL schemes are supported:

        - `redis://` creates a TCP socket connection. See more at:
          <https://www.iana.org/assignments/uri-schemes/prov/redis>
        - `rediss://` creates a SSL wrapped TCP socket connection. See more at:
          <https://www.iana.org/assignments/uri-schemes/prov/rediss>
        - ``unix://``: creates a Unix Domain Socket connection.

        The username, password, hostname, path and all querystring values
        are passed through urllib.parse.unquote in order to replace any
        percent-encoded values with their corresponding characters.

        There are several ways to specify a database number. The first value
        found will be used:
            1. A ``db`` querystring option, e.g. redis://localhost?db=0
            2. If using the redis:// or rediss:// schemes, the path argument
               of the url, e.g. redis://localhost/0
            3. A ``db`` keyword argument to this function.

        If none of these options are specified, the default db=0 is used.

        All querystring options are cast to their appropriate Python types.
        Boolean arguments can be specified with string values "True"/"False"
        or "Yes"/"No". Values that cannot be properly cast cause a
        ``ValueError`` to be raised. Once parsed, the querystring arguments
        and keyword arguments are passed to the ``ConnectionPool``'s
        class initializer. In the case of conflicting arguments, querystring
        arguments always win.
        """
        url_options = parse_url(url)
        kwargs.update(url_options)
        return cls(**kwargs)

    def __init__(
        self,
        connection_class: type[Connection] = Connection,
        max_connections: int | None = None,
        **connection_kwargs,
    ):
        max_connections = max_connections or 2**31
        if not isinstance(max_connections, int) or max_connections < 0:
            raise ValueError('"max_connections" must be a positive integer')

        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
        self.max_connections = max_connections

        # a lock to protect the critical section in _checkpid().
        # this lock is acquired when the process id changes, such as
        # after a fork. during this time, multiple threads in the child
        # process could attempt to acquire this lock. the first thread
        # to acquire the lock will reset the data structures and lock
        # object of this pool. subsequent threads acquiring this lock
        # will notice the first thread already did the work and simply
        # release the lock.
        self._lock = asyncio.Lock()
        self._created_connections: int
        self._available_connections: list[Connection]
        self._in_use_connections: set[Connection]
        self.reset()  # lgtm [py/init-calls-subclass]
        self.encoder_class = self.connection_kwargs.get("encoder_class", Encoder)

    def __repr__(self):
        return (
            f"{self.__class__.__name__}"
            f"<{self.connection_class(**self.connection_kwargs)!r}>"
        )

    def reset(self):
        self._lock = asyncio.Lock()
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()

        # this must be the last operation in this method. while reset() is
        # called when holding _fork_lock, other threads in this process
        # can call _checkpid() which compares self.pid and os.getpid() without
        # holding any lock (for performance reasons). keeping this assignment
        # as the last operation ensures that those other threads will also
        # notice a pid difference and block waiting for the first thread to
        # release _fork_lock. when each of these threads eventually acquire
        # _fork_lock, they will notice that another thread already called
        # reset() and they will immediately release _fork_lock and continue on.
        self.pid = os.getpid()

    def _checkpid(self):
        # _checkpid() attempts to keep ConnectionPool fork-safe on modern
        # systems. this is called by all ConnectionPool methods that
        # manipulate the pool's state such as get_connection() and release().
        #
        # _checkpid() determines whether the process has forked by comparing
        # the current process id to the process id saved on the ConnectionPool
        # instance. if these values are the same, _checkpid() simply returns.
        #
        # when the process ids differ, _checkpid() assumes that the process
        # has forked and that we're now running in the child process. the child
        # process cannot use the parent's file descriptors (e.g., sockets).
        # therefore, when _checkpid() sees the process id change, it calls
        # reset() in order to reinitialize the child's ConnectionPool. this
        # will cause the child to make all new connection objects.
        #
        # _checkpid() is protected by self._fork_lock to ensure that multiple
        # threads in the child process do not call reset() multiple times.
        #
        # there is an extremely small chance this could fail in the following
        # scenario:
        #   1. process A calls _checkpid() for the first time and acquires
        #      self._fork_lock.
        #   2. while holding self._fork_lock, process A forks (the fork()
        #      could happen in a different thread owned by process A)
        #   3. process B (the forked child process) inherits the
        #      ConnectionPool's state from the parent. that state includes
        #      a locked _fork_lock. process B will not be notified when
        #      process A releases the _fork_lock and will thus never be
        #      able to acquire the _fork_lock.
        #
        # to mitigate this possible deadlock, _checkpid() will only wait 5
        # seconds to acquire _fork_lock. if _fork_lock cannot be acquired in
        # that time it is assumed that the child is deadlocked and a
        # redis.ChildDeadlockedError error is raised.
        if self.pid != os.getpid():
            acquired = self._fork_lock.acquire(timeout=5)
            if not acquired:
                raise ChildDeadlockedError
            # reset() the instance for the new process if another thread
            # hasn't already done so
            try:
                if self.pid != os.getpid():
                    self.reset()
            finally:
                self._fork_lock.release()

    async def get_connection(self, command_name, *keys, **options):
        """Get a connection from the pool"""
        self._checkpid()
        async with self._lock:
            try:
                connection = self._available_connections.pop()
            except IndexError:
                connection = self.make_connection()
            self._in_use_connections.add(connection)

        try:
            # ensure this connection is connected to Redis
            await connection.connect()
            # connections that the pool provides should be ready to send
            # a command. if not, the connection was either returned to the
            # pool before all data has been read or the socket has been
            # closed. either way, reconnect and verify everything is good.
            try:
                if await connection.can_read():
                    raise ConnectionError("Connection has data") from None
            except ConnectionError:
                # await connection.disconnect()
                # await connection.connect()
                if await connection.can_read():
                    raise ConnectionError("Connection not ready") from None
        except BaseException:
            # release the connection back to the pool so that we don't
            # leak it
            await self.release(connection)
            raise

        return connection

    def get_encoder(self):
        """Return an encoder based on encoding settings"""
        kwargs = self.connection_kwargs
        return self.encoder_class(
            encoding=kwargs.get("encoding", "utf-8"),
            encoding_errors=kwargs.get("encoding_errors", "strict"),
            decode_responses=kwargs.get("decode_responses", False),
        )

    def make_connection(self):
        """Create a new connection"""
        if self._created_connections >= self.max_connections:
            raise ConnectionError("Too many connections")
        self._created_connections += 1
        return self.connection_class(**self.connection_kwargs)

    async def release(self, connection: Connection):
        """Releases the connection back to the pool"""
        self._checkpid()
        async with self._lock:
            try:
                self._in_use_connections.remove(connection)
            except KeyError:
                # Gracefully fail when a connection is returned to this pool
                # that the pool doesn't actually own
                pass

            if self.owns_connection(connection):
                self._available_connections.append(connection)
            else:
                # pool doesn't own this connection. do not add it back
                # to the pool and decrement the count so that another
                # connection can take its place if needed
                self._created_connections -= 1
                await connection.disconnect()
                return

    def owns_connection(self, connection: Connection):
        return connection.pid == self.pid

    async def disconnect(self, inuse_connections: bool = True):
        """
        Disconnects connections in the pool

        If ``inuse_connections`` is True, disconnect connections that are
        current in use, potentially by other tasks. Otherwise only disconnect
        connections that are idle in the pool.
        """
        self._checkpid()
        async with self._lock:
            if inuse_connections:
                connections: Iterable[Connection] = chain(
                    self._available_connections, self._in_use_connections
                )
            else:
                connections = self._available_connections
            resp = await asyncio.gather(
                *(connection.disconnect() for connection in connections),
                return_exceptions=True,
            )
            exc = next((r for r in resp if isinstance(r, BaseException)), None)
            if exc:
                raise exc


class BlockingConnectionPool(ConnectionPool):
    """
    Thread-safe blocking connection pool::

        >>> from aioredis.client import Redis
        >>> client = Redis(connection_pool=BlockingConnectionPool())

    It performs the same function as the default
    :py:class:`~redis.ConnectionPool` implementation, in that,
    it maintains a pool of reusable connections that can be shared by
    multiple redis clients (safely across threads if required).

    The difference is that, in the event that a client tries to get a
    connection from the pool when all of connections are in use, rather than
    raising a :py:class:`~redis.ConnectionError` (as the default
    :py:class:`~redis.ConnectionPool` implementation does), it
    makes the client wait ("blocks") for a specified number of seconds until
    a connection becomes available.

    Use ``max_connections`` to increase / decrease the pool size::

        >>> pool = BlockingConnectionPool(max_connections=10)

    Use ``timeout`` to tell it either how many seconds to wait for a connection
    to become available, or to block forever:

        >>> # Block forever.
        >>> pool = BlockingConnectionPool(timeout=None)

        >>> # Raise a ``ConnectionError`` after five seconds if a connection is
        >>> # not available.
        >>> pool = BlockingConnectionPool(timeout=5)
    """

    def __init__(
        self,
        max_connections: int = 50,
        timeout: int | None = 20,
        connection_class: type[Connection] = Connection,
        queue_class: type[asyncio.Queue] = asyncio.LifoQueue,
        **connection_kwargs,
    ):

        self.queue_class = queue_class
        self.timeout = timeout
        self._connections: list[Connection]
        super().__init__(
            connection_class=connection_class,
            max_connections=max_connections,
            **connection_kwargs,
        )

    def reset(self):
        # Create and fill up a thread safe queue with ``None`` values.
        self.pool = self.queue_class(self.max_connections)
        while True:
            try:
                self.pool.put_nowait(None)
            except asyncio.QueueFull:
                break

        # Keep a list of actual connection instances so that we can
        # disconnect them later.
        self._connections = []

        # this must be the last operation in this method. while reset() is
        # called when holding _fork_lock, other threads in this process
        # can call _checkpid() which compares self.pid and os.getpid() without
        # holding any lock (for performance reasons). keeping this assignment
        # as the last operation ensures that those other threads will also
        # notice a pid difference and block waiting for the first thread to
        # release _fork_lock. when each of these threads eventually acquire
        # _fork_lock, they will notice that another thread already called
        # reset() and they will immediately release _fork_lock and continue on.
        self.pid = os.getpid()

    def make_connection(self):
        """Make a fresh connection."""
        connection = self.connection_class(**self.connection_kwargs)
        self._connections.append(connection)
        return connection

    async def get_connection(self, command_name, *keys, **options):
        """
        Get a connection, blocking for ``self.timeout`` until a connection
        is available from the pool.

        If the connection returned is ``None`` then creates a new connection.
        Because we use a last-in first-out queue, the existing connections
        (having been returned to the pool after the initial ``None`` values
        were added) will be returned before ``None`` values. This means we only
        create new connections when we need to, i.e.: the actual number of
        connections will only increase in response to demand.
        """
        # Make sure we haven't changed process.
        self._checkpid()

        # Try and get a connection from the pool. If one isn't available within
        # self.timeout then raise a ``ConnectionError``.
        connection = None
        try:
            async with async_timeout.timeout(self.timeout):
                connection = await self.pool.get()
        except (asyncio.QueueEmpty, asyncio.TimeoutError):
            # Note that this is not caught by the redis client and will be
            # raised unless handled by application code. If you want never to
            raise ConnectionError("No connection available.")

        # If the ``connection`` is actually ``None`` then that's a cue to make
        # a new connection to add to the pool.
        if connection is None:
            connection = self.make_connection()

        try:
            # ensure this connection is connected to Redis
            await connection.connect()
            # connections that the pool provides should be ready to send
            # a command. if not, the connection was either returned to the
            # pool before all data has been read or the socket has been
            # closed. either way, reconnect and verify everything is good.
            try:
                if await connection.can_read():
                    raise ConnectionError("Connection has data") from None
            except ConnectionError:
                await connection.disconnect()
                await connection.connect()
                if await connection.can_read():
                    raise ConnectionError("Connection not ready") from None
        except BaseException:
            # release the connection back to the pool so that we don't leak it
            await self.release(connection)
            raise

        return connection

    async def release(self, connection: Connection):
        """Releases the connection back to the pool."""
        # Make sure we haven't changed process.
        self._checkpid()
        if not self.owns_connection(connection):
            # pool doesn't own this connection. do not add it back
            # to the pool. instead add a None value which is a placeholder
            # that will cause the pool to recreate the connection if
            # its needed.
            await connection.disconnect()
            self.pool.put_nowait(None)
            return

        # Put the connection back into the pool.
        try:
            self.pool.put_nowait(connection)
        except asyncio.QueueFull:
            # perhaps the pool has been reset() after a fork? regardless,
            # we don't want this connection
            pass

    async def disconnect(self, inuse_connections: bool = True):
        """Disconnects all connections in the pool."""
        self._checkpid()
        async with self._lock:
            resp = await asyncio.gather(
                *(connection.disconnect() for connection in self._connections),
                return_exceptions=True,
            )
            exc = next((r for r in resp if isinstance(r, BaseException)), None)
            if exc:
                raise exc
