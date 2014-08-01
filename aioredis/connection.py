import asyncio
import hiredis
from functools import partial
from collections import deque

from .util import encode_command
from .errors import RedisError, ProtocolError, ReplyError


__all__ = ['create_connection', 'RedisConnection']

MAX_CHUNK_SIZE = 65536
_NOTSET = object()


@asyncio.coroutine
def create_connection(address, *, db=None, password=None,
                      encoding=None, loop=None):
    """Creates redis connection.

    Opens connection to Redis server specified by address argument.
    Address argument is similar to socket address argument, ie:
    * when address is a tuple it represents (host, port) pair;
    * when address is a str it represents unix domain socket path.
    (no other address formats supported)

    Encoding argument can be used to decode byte-replies to strings.
    By default no decoding is done.

    Return value is RedisConnection instance.

    This function is a coroutine.
    """
    assert isinstance(address, (tuple, list, str)), "tuple or str expected"

    if isinstance(address, (list, tuple)):
        host, port = address
        reader, writer = yield from asyncio.open_connection(
            host, port, loop=loop)
    else:
        reader, writer = yield from asyncio.open_unix_connection(
            address, loop=loop)
    conn = RedisConnection(reader, writer, encoding=encoding, loop=loop)

    if password is not None:
        yield from conn.auth(password)
    if db is not None:
        yield from conn.select(db)
    return conn


class RedisConnection:
    """Redis connection."""

    def __init__(self, reader, writer, *, encoding=None, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self._reader = reader
        self._writer = writer
        self._loop = loop
        self._waiters = deque()
        self._parser = hiredis.Reader(protocolError=ProtocolError,
                                      replyError=ReplyError)
        self._reader_task = asyncio.Task(self._read_data(), loop=self._loop)
        self._db = 0
        self._closing = False
        self._closed = False
        self._in_transaction = False
        self._transaction_error = None
        self._encoding = encoding

    def __repr__(self):
        return '<RedisConnection [db:{}]>'.format(self._db)

    @asyncio.coroutine
    def _read_data(self):
        """Response reader task."""
        while not self._reader.at_eof():
            data = yield from self._reader.read(MAX_CHUNK_SIZE)
            self._parser.feed(data)
            while True:
                try:
                    obj = self._parser.gets()
                except ProtocolError as exc:
                    # ProtocolError is fatal
                    # so connection must be closed
                    self._closing = True
                    self._loop.call_soon(self._do_close, exc)
                    if self._in_transaction:
                        self._transaction_error = exc
                    return
                else:
                    if obj is False:
                        break
                    try:
                        waiter, encoding = self._waiters.popleft()
                    except IndexError:
                        raise   # this should not happen
                    if isinstance(obj, RedisError):
                        waiter.set_exception(obj)
                        if self._in_transaction:
                            self._transaction_error = obj
                    else:
                        if encoding is not None and isinstance(obj, bytes):
                            try:
                                obj = obj.decode(encoding)
                            except Exception as exc:
                                waiter.set_exception(exc)
                                continue
                        waiter.set_result(obj)
        self._closing = True
        self._loop.call_soon(self._do_close, None)

    def execute(self, command, *args, encoding=_NOTSET):
        """Executes redis command and returns Future waiting for the answer.

        Raises:
        * TypeError if any of args can not be encoded as bytes.
        * ReplyError on redis '-ERR' resonses.
        * ProtocolError when response can not be decoded meaning connection
          is broken.
        """
        assert self._reader and not self._reader.at_eof(), (
            "Connection closed or corrupted")
        if encoding is _NOTSET:
            encoding = self._encoding
        command = command.upper().strip()
        data = encode_command(command, *args)
        fut = asyncio.Future(loop=self._loop)
        self._waiters.append((fut, encoding))
        self._writer.write(data)
        if command in ('SELECT', b'SELECT'):
            fut.add_done_callback(partial(self._set_db, args=args))
        elif command in ('MULTI', b'MULTI'):
            fut.add_done_callback(self._start_transaction)
        elif command in ('EXEC', b'EXEC', 'DISCARD', b'DISCARD'):
            fut.add_done_callback(self._end_transaction)
        return fut

    def close(self):
        """Close connection."""
        self._do_close(None)

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
        while self._waiters:
            waiter, *spam = self._waiters.popleft()
            if exc is None:
                waiter.cancel()
            else:
                waiter.set_exception(exc)

    @property
    def closed(self):
        """True if connection is closed."""
        closed = self._closing or self._closed
        if not closed and self._reader and self._reader.at_eof():
            self._closing = closed = True
            self._loop.call_soon(self._do_close, None)
        return closed

    @property
    def db(self):
        """Currently selected db index."""
        return self._db

    @property
    def encoding(self):
        """Current set codec or None."""
        return self._encoding

    @asyncio.coroutine
    def select(self, db):
        """Change the selected database for the current connection.

        This method is a coroutine.
        """
        if not isinstance(db, int):
            raise TypeError("DB must be of int type, not {!r}".format(db))
        if db < 0:
            raise ValueError("DB must be greater or equal 0, got {!r}"
                             .format(db))
        yield from self.execute('SELECT', db)
        return True

    def _set_db(self, fut, args):
        try:
            ok = fut.result()
        except Exception:
            pass
        else:
            assert ok in {b'OK', 'OK'}, ok
            self._db = args[0]

    def _start_transaction(self, fut):
        try:
            fut.result()
        except Exception:
            pass
        else:
            assert not self._in_transaction
            self._in_transaction = True
            self._transaction_error = None

    def _end_transaction(self, fut):
        try:
            fut.result()
        except Exception:
            pass
        else:
            assert self._in_transaction
            self._in_transaction = False
            self._transaction_error = None

    @property
    def in_transaction(self):
        """Set to True when MULTI command was issued."""
        return self._in_transaction

    @asyncio.coroutine
    def auth(self, password):
        """Authenticate to server."""
        ok = yield from self.execute('AUTH', password)
        return ok == b'OK'
