import asyncio

from aioredis.connection import create_connection
from .keys import KeysCommandsMixin


__all__ = ['create_redis', 'Redis']


class Redis(KeysCommandsMixin):
    """High-level Redis interface.

    Gathers in one place Redis commands implemented in mixins.

    This class contains Connection
    """

    def __init__(self, connection):
        self._conn = connection
        self._db = 0

    def close(self):
        self._conn.close()

    @property
    def db(self):
        return self._db

    @asyncio.coroutine
    def auth(self, password):
        """Authenticate to server.
        """
        yield from self._conn.execute('AUTH', password)

    @asyncio.coroutine
    def echo(self, message):
        """Echo the given string.
        """
        yield from self._conn.execute('ECHO', message)

    @asyncio.coroutine
    def ping(self):
        """Ping the server.
        """
        yield from self._conn.execute('PING')

    @asyncio.coroutine
    def quit(self):
        """Close the connection.
        """
        yield from self._conn.execute('QUIT')
        self.close()

    @asyncio.coroutine
    def select(self, db):
        """Change the selected database for the current connection.
        """
        if not isinstance(db, int):
            raise TypeError("DB must be of int type, not {!r}".format(db))
        if db < 0:
            raise ValueError("DB must be greater or equal 0, got {!r}"
                             .format(db))
        ok = yield from self._conn.execute('SELECT', db)
        return ok == b'OK'


@asyncio.coroutine
def create_redis(address, db=0, auth=None, *,
                 commands_factory=Redis, loop=None):
    """
    """
    conn = yield from create_connection(address, loop=loop)
    redis = commands_factory(conn)
    if auth is not None:
        yield from redis.auth(auth)
    if db > 0:
        yield from redis.select(db)
    return redis
