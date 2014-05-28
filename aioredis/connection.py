import asyncio
import hiredis

from .util import encode_command


__all__ = ['create_connection', 'RedisConnection']


@asyncio.coroutine
def create_connection(address, db=0, auth=None, *, loop=None):
    """Creates redis connection.

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
        pass
    if db is not None:
        result = yield from conn.select(db)
        assert result is True, db
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
        self._parser = hiredis.Reader()
        self._reader_task = asyncio.Task(self._read_data(), loop=self._loop)
        self._db = None

    def __repr__(self):
        return '<RedisConnection>'

    @asyncio.coroutine
    def _execute(self, fut, data):
        """Low-level method for redis command execution.
        """
        assert isinstance(data, (bytes, bytearray, memoryview))
        try:
            self._writer.write(data)
            yield from self._writer.drain()
        # TODO: handle specific exceptions (like connection lost and reconnect)
        except Exception as exc:
            fut.set_exception(exc)
        else:
            yield from self._waiters.put(fut)

    @asyncio.coroutine
    def _read_data(self):
        while not self._reader.at_eof():
            data = yield from self._reader.readline()
            self._parser.feed(data)
            while True:
                obj = self._parser.gets()
                if obj is False:
                    break
                waiter = yield from self._waiters.get()
                waiter.set_result(obj)

    def execute(self, cmd, *args):
        """Executes redis command and returns Future waiting for the answer.

        Raises TypeError if any of args can not be encoded as bytes.
        """
        data = encode_command(cmd, *args)
        fut = asyncio.Future(loop=self._loop)
        asyncio.Task(self._execute(fut, data), loop=self._loop)
        return fut

    def close(self):
        self._writer.transport.close()
        self._reader_task.cancel()
        self._reader_task = None
        self._writer = None
        self._reader = None

    # no use cases yet
    # @property
    # def transport(self):
    #     """Transport instance.
    #     """
    #     return self._writer.transport

    @property
    def db(self):
        return self._db

    @asyncio.coroutine
    def select(self, db):
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
