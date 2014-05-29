import asyncio
import hiredis

from .util import encode_command
from .errors import RedisError, ProtocolError, ReplyError


__all__ = ['create_connection', 'RedisConnection']


@asyncio.coroutine
def create_connection(address, db=0, auth=None, *, loop=None):
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

    if auth is not None:
        yield from conn.execute('AUTH', auth)
    if db and db > 0:
        yield from conn.execute('SELECT', db)
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

    def __repr__(self):
        return '<RedisConnection>'  # make more verbose

    @asyncio.coroutine
    def _read_data(self):
        """
        """
        while not self._reader.at_eof():
            data = yield from self._reader.readline()
            self._parser.feed(data)
            while True:
                try:
                    obj = self._parser.gets()
                except ProtocolError as exc:
                    # ProtocolError is fatal
                    # so connection must be closed
                    waiter = yield from self._waiters.get()
                    waiter.set_exception(exc)
                    self._loop.call_soon(self.close)
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
    def execute(self, cmd, *args):
        """Executes redis command and returns Future waiting for the answer.

        Raises TypeError if any of args can not be encoded as bytes.
        """
        data = encode_command(cmd, *args)
        self._writer.write(data)
        yield from self._writer.drain()
        fut = asyncio.Future(loop=self._loop)
        yield from self._waiters.put(fut)
        return (yield from fut)

    def close(self):
        """Close connection.
        """
        self._writer.transport.close()
        self._reader_task.cancel()
        self._reader_task = None
        self._writer = None
        self._reader = None
        # TODO: discard all _waiters

    @property
    def db(self):
        return self._db

    @asyncio.coroutine
    def select(self, db):
        # TODO: move this to high-level part...
        """Executes SELECT command.

        This method is a coroutine.
        """
        if not isinstance(db, int):
            raise TypeError("DB must be of int type, not {!r}".format(db))
        if db < 0:
            raise ValueError("DB must be greater equal 0, got {!r}".format(db))
        resp = yield from self.execute('select', str(db))
        # TODO: set db to self._db
        if resp == b'OK':
            self._db = db
            return True
        else:
            return False
