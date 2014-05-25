import asyncio

from .protocol import RedisProtocol


__all__ = ['create_connection', 'RedisConnection']


@asyncio.coroutine
def create_connection(address, db=0, *, loop=None):
    """Creates redis connection.

    This function is a coroutine.
    """
    assert isinstance(address, (tuple, str)), "tuple or str expected"
    if loop is None:
        loop = asyncio.get_event_loop()
    if isinstance(address, tuple):
        host, port = address
        _, proto = yield from loop.create_connection(
            lambda: RedisProtocol(loop=loop), host, port)
    else:
        _, proto = yield from loop.create_unix_connection(
            lambda: RedisProtocol(loop=loop), address)
    # waiter = asyncio.Future(loop=loop)

    conn = RedisConnection(proto, db, loop=loop)

    # fut = yield from waiter
    # ok = yield from fut
    # assert ok == b'OK'

    return conn


class RedisConnection:
    """Redis connection.
    """

    def __init__(self, protocol, db=0, waiter=None, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self._protocol = protocol
        self._db = db
        self._loop = loop
        if waiter is not None:
            fut = self.select(db)
            fut.add_done_callback(waiter.set_result)

    def __repr__(self):
        return '<RedisConnection>'

    @property
    def transport(self):
        """Transport instance.
        """
        return self._protocol.transport

    @asyncio.coroutine
    def execute(self, cmd, *args):
        """Execute command.

        Wrapper around protocol's execute method.

        This method is a coroutine.
        """
        # XXX: simply proxing command to protocol?
        return self._protocol.execute(cmd, *args)

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
        return resp == b'OK'
