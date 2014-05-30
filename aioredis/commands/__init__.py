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

    def close(self):
        self._conn.close()

    @property
    def db(self):
        """Currently selected db index.
        """
        return self._conn.db

    @property
    def connection(self):
        """RedisConnection instance.
        """
        return self._conn

    @asyncio.coroutine
    def auth(self, password):
        """Authenticate to server.

        This method wraps call to connection.auth()
        """
        return self._conn.auth(password)

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

        This method wraps call to connection.select()
        """
        return self._conn.select(db)


@asyncio.coroutine
def create_redis(address, db=None, password=None, *,
                 commands_factory=Redis, loop=None):
    """Creates high-level Redis interface.

    This function is a coroutine.
    """
    conn = yield from create_connection(address, db, password, loop=loop)
    return commands_factory(conn)
