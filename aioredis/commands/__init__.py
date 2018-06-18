from aioredis.connection import create_connection
from aioredis.pool import create_pool
from aioredis.util import _NOTSET, wait_ok
from aioredis.abc import AbcPool
from .generic import GenericCommandsMixin
from .string import StringCommandsMixin
from .hash import HashCommandsMixin
from .hyperloglog import HyperLogLogCommandsMixin
from .set import SetCommandsMixin
from .sorted_set import SortedSetCommandsMixin
from .transaction import TransactionsCommandsMixin, Pipeline, MultiExec
from .list import ListCommandsMixin
from .scripting import ScriptingCommandsMixin
from .server import ServerCommandsMixin
from .pubsub import PubSubCommandsMixin
from .cluster import ClusterCommandsMixin
from .geo import GeoCommandsMixin, GeoPoint, GeoMember
from .streams import StreamCommandsMixin

__all__ = [
    'create_redis',
    'create_redis_pool',
    'Redis',
    'Pipeline',
    'MultiExec',
    'GeoPoint',
    'GeoMember',
]


class Redis(GenericCommandsMixin, StringCommandsMixin,
            HyperLogLogCommandsMixin, SetCommandsMixin,
            HashCommandsMixin, TransactionsCommandsMixin,
            SortedSetCommandsMixin, ListCommandsMixin,
            ScriptingCommandsMixin, ServerCommandsMixin,
            PubSubCommandsMixin, ClusterCommandsMixin,
            GeoCommandsMixin, StreamCommandsMixin):
    """High-level Redis interface.

    Gathers in one place Redis commands implemented in mixins.

    For commands details see: http://redis.io/commands/#connection
    """
    def __init__(self, pool_or_conn):
        self._pool_or_conn = pool_or_conn

    def __repr__(self):
        return '<{} {!r}>'.format(self.__class__.__name__, self._pool_or_conn)

    def execute(self, command, *args, **kwargs):
        return self._pool_or_conn.execute(command, *args, **kwargs)

    def close(self):
        """Close client connections."""
        self._pool_or_conn.close()

    async def wait_closed(self):
        """Coroutine waiting until underlying connections are closed."""
        await self._pool_or_conn.wait_closed()

    @property
    def db(self):
        """Currently selected db index."""
        return self._pool_or_conn.db

    @property
    def encoding(self):
        """Current set codec or None."""
        return self._pool_or_conn.encoding

    @property
    def connection(self):
        """Either :class:`aioredis.RedisConnection`,
        or :class:`aioredis.ConnectionsPool` instance.
        """
        return self._pool_or_conn

    @property
    def address(self):
        """Redis connection address (if applicable)."""
        return self._pool_or_conn.address

    @property
    def in_transaction(self):
        """Set to True when MULTI command was issued."""
        # XXX: this must be bound to real connection
        return self._pool_or_conn.in_transaction

    @property
    def closed(self):
        """True if connection is closed."""
        return self._pool_or_conn.closed

    def auth(self, password):
        """Authenticate to server.

        This method wraps call to :meth:`aioredis.RedisConnection.auth()`
        """
        return self._pool_or_conn.auth(password)

    def echo(self, message, *, encoding=_NOTSET):
        """Echo the given string."""
        return self.execute('ECHO', message, encoding=encoding)

    def ping(self, message=_NOTSET, *, encoding=_NOTSET):
        """Ping the server.

        Accept optional echo message.
        """
        if message is not _NOTSET:
            args = (message,)
        else:
            args = ()
        return self.execute('PING', *args, encoding=encoding)

    def quit(self):
        """Close the connection."""
        # TODO: warn when using pool
        return self.execute('QUIT')

    def select(self, db):
        """Change the selected database for the current connection.

        This method wraps call to :meth:`aioredis.RedisConnection.select()`
        """
        return self._pool_or_conn.select(db)

    def swapdb(self, from_index, to_index):
        return wait_ok(self.execute(b'SWAPDB', from_index, to_index))

    def __await__(self):
        if isinstance(self._pool_or_conn, AbcPool):
            conn = yield from self._pool_or_conn.acquire().__await__()
            release = self._pool_or_conn.release
        else:
            # TODO: probably a lock is needed here if _pool_or_conn
            #       is Connection instance.
            conn = self._pool_or_conn
            release = None
        return ContextRedis(conn, release)


class ContextRedis(Redis):
    """An instance of Redis class bound to single connection."""

    def __init__(self, conn, release_cb=None):
        super().__init__(conn)
        self._release_callback = release_cb

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        if self._release_callback is not None:
            conn, self._pool_or_conn = self._pool_or_conn, None
            self._release_callback(conn)

    def __await__(self):
        return ContextRedis(self._pool_or_conn)
        yield


async def create_redis(address, *, db=None, password=None, ssl=None,
                       encoding=None, commands_factory=Redis,
                       parser=None, timeout=None,
                       connection_cls=None, loop=None):
    """Creates high-level Redis interface.

    This function is a coroutine.
    """
    conn = await create_connection(address, db=db,
                                   password=password,
                                   ssl=ssl,
                                   encoding=encoding,
                                   parser=parser,
                                   timeout=timeout,
                                   connection_cls=connection_cls,
                                   loop=loop)
    return commands_factory(conn)


async def create_redis_pool(address, *, db=None, password=None, ssl=None,
                            encoding=None, commands_factory=Redis,
                            minsize=1, maxsize=10, parser=None,
                            timeout=None, pool_cls=None,
                            connection_cls=None, loop=None):
    """Creates high-level Redis interface.

    This function is a coroutine.
    """
    pool = await create_pool(address, db=db,
                             password=password,
                             ssl=ssl,
                             encoding=encoding,
                             minsize=minsize,
                             maxsize=maxsize,
                             parser=parser,
                             create_connection_timeout=timeout,
                             pool_cls=pool_cls,
                             connection_cls=connection_cls,
                             loop=loop)
    return commands_factory(pool)
