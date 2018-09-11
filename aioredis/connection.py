import types
import asyncio
import socket
from functools import partial
from collections import deque
from contextlib import contextmanager

from .util import (
    encode_command,
    wait_ok,
    _NOTSET,
    _set_result,
    _set_exception,
    coerced_keys_dict,
    decode,
    parse_url,
    )
from .parser import Reader
from .stream import open_connection, open_unix_connection
from .errors import (
    ConnectionClosedError,
    ConnectionForcedCloseError,
    RedisError,
    ProtocolError,
    ReplyError,
    WatchVariableError,
    ReadOnlyError,
    MaxClientsError
    )
from .pubsub import Channel
from .abc import AbcChannel
from .abc import AbcConnection
from .log import logger


__all__ = ['create_connection', 'RedisConnection']

MAX_CHUNK_SIZE = 65536

_PUBSUB_COMMANDS = (
    'SUBSCRIBE', b'SUBSCRIBE',
    'PSUBSCRIBE', b'PSUBSCRIBE',
    'UNSUBSCRIBE', b'UNSUBSCRIBE',
    'PUNSUBSCRIBE', b'PUNSUBSCRIBE',
    )


async def create_connection(address, *, db=None, password=None, ssl=None,
                            encoding=None, parser=None, loop=None,
                            timeout=None, connection_cls=None):
    """Creates redis connection.

    Opens connection to Redis server specified by address argument.
    Address argument can be one of the following:
    * A tuple representing (host, port) pair for TCP connections;
    * A string representing either Redis URI or unix domain socket path.

    SSL argument is passed through to asyncio.create_connection.
    By default SSL/TLS is not used.

    By default any timeout is applied at the connection stage, however
    you can set a limitted time used trying to open a connection via
    the `timeout` Kw.

    Encoding argument can be used to decode byte-replies to strings.
    By default no decoding is done.

    Parser parameter can be used to pass custom Redis protocol parser class.
    By default hiredis.Reader is used (unless it is missing or platform
    is not CPython).

    Return value is RedisConnection instance or a connection_cls if it is
    given.

    This function is a coroutine.
    """
    assert isinstance(address, (tuple, list, str)), "tuple or str expected"
    if isinstance(address, str):
        address, options = parse_url(address)
        logger.debug("Parsed Redis URI %r", address)
        db = options.setdefault('db', db)
        password = options.setdefault('password', password)
        encoding = options.setdefault('encoding', encoding)
        timeout = options.setdefault('timeout', timeout)
        if 'ssl' in options:
            assert options['ssl'] or (not options['ssl'] and not ssl), (
                "Conflicting ssl options are set", options['ssl'], ssl)
            ssl = ssl or options['ssl']

    if timeout is not None and timeout <= 0:
        raise ValueError("Timeout has to be None or a number greater than 0")

    if connection_cls:
        assert issubclass(connection_cls, AbcConnection),\
                "connection_class does not meet the AbcConnection contract"
        cls = connection_cls
    else:
        cls = RedisConnection

    if loop is None:
        loop = asyncio.get_event_loop()

    if isinstance(address, (list, tuple)):
        host, port = address
        logger.debug("Creating tcp connection to %r", address)
        reader, writer = await asyncio.wait_for(open_connection(
            host, port, limit=MAX_CHUNK_SIZE, ssl=ssl, loop=loop),
            timeout, loop=loop)
        sock = writer.transport.get_extra_info('socket')
        if sock is not None:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            address = sock.getpeername()
        address = tuple(address[:2])
    else:
        logger.debug("Creating unix connection to %r", address)
        reader, writer = await asyncio.wait_for(open_unix_connection(
            address, ssl=ssl, limit=MAX_CHUNK_SIZE, loop=loop),
            timeout, loop=loop)
        sock = writer.transport.get_extra_info('socket')
        if sock is not None:
            address = sock.getpeername()

    conn = cls(reader, writer, encoding=encoding,
               address=address, parser=parser,
               loop=loop)

    try:
        if password is not None:
            await conn.auth(password)
        if db is not None:
            await conn.select(db)
    except Exception:
        conn.close()
        await conn.wait_closed()
        raise
    return conn


class RedisConnection(AbcConnection):
    """Redis connection."""

    def __init__(self, reader, writer, *, address, encoding=None,
                 parser=None, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        if parser is None:
            parser = Reader
        assert callable(parser), (
            "Parser argument is not callable", parser)
        self._reader = reader
        self._writer = writer
        self._address = address
        self._loop = loop
        self._waiters = deque()
        self._reader.set_parser(
            parser(protocolError=ProtocolError, replyError=ReplyError)
        )
        self._reader_task = asyncio.ensure_future(self._read_data(),
                                                  loop=self._loop)
        self._close_msg = None
        self._db = 0
        self._closing = False
        self._closed = False
        self._close_state = asyncio.Event()
        self._reader_task.add_done_callback(lambda x: self._close_state.set())
        self._in_transaction = None
        self._transaction_error = None  # XXX: never used?
        self._in_pubsub = 0
        self._pubsub_channels = coerced_keys_dict()
        self._pubsub_patterns = coerced_keys_dict()
        self._encoding = encoding
        self._pipeline_buffer = None

    def __repr__(self):
        return '<RedisConnection [db:{}]>'.format(self._db)

    async def _read_data(self):
        """Response reader task."""
        last_error = ConnectionClosedError(
            "Connection has been closed by server")
        while not self._reader.at_eof():
            try:
                obj = await self._reader.readobj()
            except asyncio.CancelledError:
                # NOTE: reader can get cancelled from `close()` method only.
                last_error = RuntimeError('this is unexpected')
                break
            except ProtocolError as exc:
                # ProtocolError is fatal
                # so connection must be closed
                if self._in_transaction is not None:
                    self._transaction_error = exc
                last_error = exc
                break
            except Exception as exc:
                # NOTE: for QUIT command connection error can be received
                #       before response
                last_error = exc
                break
            else:
                if (obj == b'' or obj is None) and self._reader.at_eof():
                    logger.debug("Connection has been closed by server,"
                                 " response: %r", obj)
                    last_error = ConnectionClosedError("Reader at end of file")
                    break

                if isinstance(obj, MaxClientsError):
                    last_error = obj
                    break
                if self._in_pubsub:
                    self._process_pubsub(obj)
                else:
                    self._process_data(obj)
        self._closing = True
        self._loop.call_soon(self._do_close, last_error)

    def _process_data(self, obj):
        """Processes command results."""
        assert len(self._waiters) > 0, (type(obj), obj)
        waiter, encoding, cb = self._waiters.popleft()
        if isinstance(obj, RedisError):
            if isinstance(obj, ReplyError):
                if obj.args[0].startswith('READONLY'):
                    obj = ReadOnlyError(obj.args[0])
            _set_exception(waiter, obj)
            if self._in_transaction is not None:
                self._transaction_error = obj
        else:
            if encoding is not None:
                try:
                    obj = decode(obj, encoding)
                except Exception as exc:
                    _set_exception(waiter, exc)
                    return
            if cb is not None:
                try:
                    obj = cb(obj)
                except Exception as exc:
                    _set_exception(waiter, exc)
                    return
            _set_result(waiter, obj)
            if self._in_transaction is not None:
                self._in_transaction.append((encoding, cb))

    def _process_pubsub(self, obj, *, process_waiters=True):
        """Processes pubsub messages."""
        kind, *args, data = obj
        if kind in (b'subscribe', b'unsubscribe'):
            chan, = args
            if process_waiters and self._in_pubsub and self._waiters:
                self._process_data(obj)
            if kind == b'unsubscribe':
                ch = self._pubsub_channels.pop(chan, None)
                if ch:
                    ch.close()
            self._in_pubsub = data
        elif kind in (b'psubscribe', b'punsubscribe'):
            chan, = args
            if process_waiters and self._in_pubsub and self._waiters:
                self._process_data(obj)
            if kind == b'punsubscribe':
                ch = self._pubsub_patterns.pop(chan, None)
                if ch:
                    ch.close()
            self._in_pubsub = data
        elif kind == b'message':
            chan, = args
            self._pubsub_channels[chan].put_nowait(data)
        elif kind == b'pmessage':
            pattern, chan = args
            self._pubsub_patterns[pattern].put_nowait((chan, data))
        elif kind == b'pong':
            if process_waiters and self._in_pubsub and self._waiters:
                self._process_data(data or b'PONG')
        else:
            logger.warning("Unknown pubsub message received %r", obj)

    @contextmanager
    def _buffered(self):
        # XXX: we must ensure that no await happens
        #   as long as we buffer commands.
        #   Probably we can set some error-raising callback on enter
        #   and remove it on exit
        #   if some await happens in between -> throw an error.
        #   This is creepy solution, 'cause some one might want to await
        #   on some other source except redis.
        #   So we must only raise error we someone tries to await
        #   pending aioredis future
        # One of solutions is to return coroutine instead of a future
        # in `execute` method.
        # In a coroutine we can check if buffering is enabled and raise error.

        # TODO: describe in docs difference in pipeline mode for
        #   conn.execute vs pipeline.execute()
        if self._pipeline_buffer is None:
            self._pipeline_buffer = bytearray()
            try:
                yield self
                buf = self._pipeline_buffer
                self._writer.write(buf)
            finally:
                self._pipeline_buffer = None
        else:
            yield self

    def execute(self, command, *args, encoding=_NOTSET):
        """Executes redis command and returns Future waiting for the answer.

        Raises:
        * TypeError if any of args can not be encoded as bytes.
        * ReplyError on redis '-ERR' responses.
        * ProtocolError when response can not be decoded meaning connection
          is broken.
        * ConnectionClosedError when either client or server has closed the
          connection.
        """
        if self._reader is None or self._reader.at_eof():
            msg = self._close_msg or "Connection closed or corrupted"
            raise ConnectionClosedError(msg)
        if command is None:
            raise TypeError("command must not be None")
        if None in args:
            raise TypeError("args must not contain None")
        command = command.upper().strip()
        is_pubsub = command in _PUBSUB_COMMANDS
        is_ping = command in ('PING', b'PING')
        if self._in_pubsub and not (is_pubsub or is_ping):
            raise RedisError("Connection in SUBSCRIBE mode")
        elif is_pubsub:
            logger.warning("Deprecated. Use `execute_pubsub` method directly")
            return self.execute_pubsub(command, *args)

        if command in ('SELECT', b'SELECT'):
            cb = partial(self._set_db, args=args)
        elif command in ('MULTI', b'MULTI'):
            cb = self._start_transaction
        elif command in ('EXEC', b'EXEC'):
            cb = partial(self._end_transaction, discard=False)
        elif command in ('DISCARD', b'DISCARD'):
            cb = partial(self._end_transaction, discard=True)
        else:
            cb = None
        if encoding is _NOTSET:
            encoding = self._encoding
        fut = self._loop.create_future()
        if self._pipeline_buffer is None:
            self._writer.write(encode_command(command, *args))
        else:
            encode_command(command, *args, buf=self._pipeline_buffer)
        self._waiters.append((fut, encoding, cb))
        return fut

    def execute_pubsub(self, command, *channels):
        """Executes redis (p)subscribe/(p)unsubscribe commands.

        Returns asyncio.gather coroutine waiting for all channels/patterns
        to receive answers.
        """
        command = command.upper().strip()
        assert command in _PUBSUB_COMMANDS, (
            "Pub/Sub command expected", command)
        if self._reader is None or self._reader.at_eof():
            raise ConnectionClosedError("Connection closed or corrupted")
        if None in set(channels):
            raise TypeError("args must not contain None")
        if not len(channels):
            raise TypeError("No channels/patterns supplied")
        is_pattern = len(command) in (10, 12)
        mkchannel = partial(Channel, is_pattern=is_pattern, loop=self._loop)
        channels = [ch if isinstance(ch, AbcChannel) else mkchannel(ch)
                    for ch in channels]
        if not all(ch.is_pattern == is_pattern for ch in channels):
            raise ValueError("Not all channels {} match command {}"
                             .format(channels, command))
        cmd = encode_command(command, *(ch.name for ch in channels))
        res = []
        for ch in channels:
            fut = self._loop.create_future()
            res.append(fut)
            cb = partial(self._update_pubsub, ch=ch)
            self._waiters.append((fut, None, cb))
        if self._pipeline_buffer is None:
            self._writer.write(cmd)
        else:
            self._pipeline_buffer.extend(cmd)
        return asyncio.gather(*res, loop=self._loop)

    def close(self):
        """Close connection."""
        self._do_close(ConnectionForcedCloseError())

    def _do_close(self, exc):
        if self._closed:
            return
        self._closed = True
        self._closing = False
        self._writer.transport.close()
        self._reader_task.cancel()
        self._reader_task = None
        self._writer = None
        self._reader = None
        self._pipeline_buffer = None

        if exc is not None:
            self._close_msg = str(exc)

        while self._waiters:
            waiter, *spam = self._waiters.popleft()
            logger.debug("Cancelling waiter %r", (waiter, spam))
            if exc is None:
                _set_exception(waiter, ConnectionForcedCloseError())
            else:
                _set_exception(waiter, exc)
        while self._pubsub_channels:
            _, ch = self._pubsub_channels.popitem()
            logger.debug("Closing pubsub channel %r", ch)
            ch.close(exc)
        while self._pubsub_patterns:
            _, ch = self._pubsub_patterns.popitem()
            logger.debug("Closing pubsub pattern %r", ch)
            ch.close(exc)

    @property
    def closed(self):
        """True if connection is closed."""
        closed = self._closing or self._closed
        if not closed and self._reader and self._reader.at_eof():
            self._closing = closed = True
            self._loop.call_soon(self._do_close, None)
        return closed

    async def wait_closed(self):
        """Coroutine waiting until connection is closed."""
        await self._close_state.wait()

    @property
    def db(self):
        """Currently selected db index."""
        return self._db

    @property
    def encoding(self):
        """Current set codec or None."""
        return self._encoding

    @property
    def address(self):
        """Redis server address, either host-port tuple or str."""
        return self._address

    def select(self, db):
        """Change the selected database for the current connection."""
        if not isinstance(db, int):
            raise TypeError("DB must be of int type, not {!r}".format(db))
        if db < 0:
            raise ValueError("DB must be greater or equal 0, got {!r}"
                             .format(db))
        fut = self.execute('SELECT', db)
        return wait_ok(fut)

    def _set_db(self, ok, args):
        assert ok in {b'OK', 'OK'}, ("Unexpected result of SELECT", ok)
        self._db = args[0]
        return ok

    def _start_transaction(self, ok):
        assert self._in_transaction is None, (
            "Connection is already in transaction", self._in_transaction)
        self._in_transaction = deque()
        self._transaction_error = None
        return ok

    def _end_transaction(self, obj, discard):
        assert self._in_transaction is not None, (
            "Connection is not in transaction", obj)
        self._transaction_error = None
        recall, self._in_transaction = self._in_transaction, None
        recall.popleft()  # ignore first (its _start_transaction)
        if discard:
            return obj
        assert isinstance(obj, list) or (obj is None and not discard), (
            "Unexpected MULTI/EXEC result", obj, recall)
        # TODO: need to be able to re-try transaction
        if obj is None:
            err = WatchVariableError("WATCH variable has changed")
            obj = [err] * len(recall)
        assert len(obj) == len(recall), (
            "Wrong number of result items in mutli-exec", obj, recall)
        res = []
        for o, (encoding, cb) in zip(obj, recall):
            if not isinstance(o, RedisError):
                try:
                    if encoding:
                        o = decode(o, encoding)
                    if cb:
                        o = cb(o)
                except Exception as err:
                    res.append(err)
                    continue
            res.append(o)
        return res

    def _update_pubsub(self, obj, *, ch):
        kind, *pattern, channel, subscriptions = obj
        self._in_pubsub, was_in_pubsub = subscriptions, self._in_pubsub
        # XXX: the channels/patterns storage should be refactored.
        #   if code which supposed to read from channel/pattern
        #   failed (exception in reader or else) than
        #   the channel object will still reside in memory
        #   and leak memory (messages will be put in queue).
        if kind == b'subscribe' and channel not in self._pubsub_channels:
            self._pubsub_channels[channel] = ch
        elif kind == b'psubscribe' and channel not in self._pubsub_patterns:
            self._pubsub_patterns[channel] = ch
        if not was_in_pubsub:
            self._process_pubsub(obj, process_waiters=False)
        return obj

    @property
    def in_transaction(self):
        """Set to True when MULTI command was issued."""
        return self._in_transaction is not None

    @property
    def in_pubsub(self):
        """Indicates that connection is in PUB/SUB mode.

        Provides the number of subscribed channels.
        """
        return self._in_pubsub

    @property
    def pubsub_channels(self):
        """Returns read-only channels dict."""
        return types.MappingProxyType(self._pubsub_channels)

    @property
    def pubsub_patterns(self):
        """Returns read-only patterns dict."""
        return types.MappingProxyType(self._pubsub_patterns)

    def auth(self, password):
        """Authenticate to server."""
        fut = self.execute('AUTH', password)
        return wait_ok(fut)
