import asyncio
import hiredis
from functools import partial

from .util import encode_command
from .errors import RedisError, ProtocolError, ReplyError


__all__ = ['create_connection', 'RedisConnection']

MAX_CHUNK_SIZE = 65536


@asyncio.coroutine
def create_connection(address, db=None, password=None, *, loop=None):
    """Creates redis connection.

    Opens connection to Redis server specified by address argument.
    Address format is similar to socket address, ie:
    * when address is a tuple it represents (host, port) pair;
    * when address is a str it represents unix domain socket path.
    (no other address formats supported)

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
    conn = RedisConnection(reader, writer, loop=loop)

    if password is not None:
        yield from conn.auth(password)
    if db is not None:
        yield from conn.select(db)
    return conn


class RedisConnection:
    """Redis connection.
    """

    def __init__(self, reader, writer, *, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self._reader = reader
        self._writer = writer
        self._loop = loop
        self._waiters = asyncio.Queue(loop=self._loop)
        self._parser = hiredis.Reader(protocolError=ProtocolError,
                                      replyError=ReplyError)
        self._reader_task = asyncio.Task(self._read_data(), loop=self._loop)
        self._db = 0
        self._closed = False

    def __repr__(self):
        return '<RedisConnection [db:{}]>'.format(self._db)

    @asyncio.coroutine
    def _read_data(self):
        """
        """
        while not self._reader.at_eof():
            data = yield from self._reader.read(MAX_CHUNK_SIZE)
            self._parser.feed(data)
            while True:
                try:
                    obj = self._parser.gets()
                except ProtocolError as exc:
                    # ProtocolError is fatal
                    # so connection must be closed
                    self._loop.call_soon(self._do_close, exc)
                    return
                else:
                    if obj is False:
                        break
                    waiter = yield from self._waiters.get()
                    if isinstance(obj, RedisError):
                        waiter.set_exception(obj)
                    else:
                        waiter.set_result(obj)

    @asyncio.coroutine
    def execute(self, command, *args):
        """Executes redis command and returns Future waiting for the answer.

        Raises:
        * TypeError if any of args can not be encoded as bytes.
        * ReplyError on redis '-ERR' resonses.
        * ProtocolError when response can not be decoded meaning connection
          is broken.
        """
        command = command.upper().strip()
        data = encode_command(command, *args)
        self._writer.write(data)
        yield from self._writer.drain()
        fut = asyncio.Future(loop=self._loop)
        yield from self._waiters.put(fut)
        if command == 'SELECT':
            fut.add_done_callback(partial(self._set_db, args=args))
        return (yield from fut)

    def close(self):
        """Close connection.
        """
        self._do_close(None)

    def _do_close(self, exc):
        self._closed = True
        self._writer.transport.close()
        self._reader_task.cancel()
        self._reader_task = None
        self._writer = None
        self._reader = None
        while self._waiters.qsize():
            waiter = self._waiters.get_nowait()
            if exc is None:
                waiter.cancel()
            else:
                waiter.set_exception(exc)

    @property
    def closed(self):
        """True if connection is closed.
        """
        # TODO: implement
        return self._closed

    @property
    def db(self):
        """Currently selected db index.
        """
        return self._db

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
            assert ok == b'OK', ok
            self._db = args[0]

    @asyncio.coroutine
    def auth(self, password):
        ok = yield from self.execute('AUTH', password)
        return ok == b'OK'
