import asyncio

from aioredis.connection import create_connection
from aioredis.util import _NOTSET
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

__all__ = [
    'create_redis', 'Redis',
    'Pipeline', 'MultiExec',
    'GeoPoint', 'GeoMember',
]


class AutoConnector(object):
    closed = False

    def __init__(self, *conn_args, **conn_kwargs):
        self._conn_args = conn_args
        self._conn_kwargs = conn_kwargs
        self._conn = None
        self._loop = conn_kwargs.get('loop')
        self._lock = asyncio.Lock(loop=self._loop)

    def __repr__(self):
        return '<AutoConnector {!r}>'.format(self._conn)

    @asyncio.coroutine
    def execute(self, *args, **kwargs):
        conn = yield from self.get_atomic_connection()
        return (yield from conn.execute(*args, **kwargs))

    @asyncio.coroutine
    def get_atomic_connection(self):
        if self._conn is None or self._conn.closed:
            with (yield from self._lock):
                if self._conn is None or self._conn.closed:
                    conn = yield from create_connection(
                        *self._conn_args, **self._conn_kwargs)
                    self._conn = conn
        return self._conn


class Redis(GenericCommandsMixin, StringCommandsMixin,
            HyperLogLogCommandsMixin, SetCommandsMixin,
            HashCommandsMixin, TransactionsCommandsMixin,
            SortedSetCommandsMixin, ListCommandsMixin,
            ScriptingCommandsMixin, ServerCommandsMixin,
            PubSubCommandsMixin, ClusterCommandsMixin,
            GeoCommandsMixin):
    """High-level Redis interface.

    Gathers in one place Redis commands implemented in mixins.

    For commands details see: http://redis.io/commands/#connection
    """

    def __init__(self, connection):
        self._conn = connection

    def __repr__(self):
        return '<Redis {!r}>'.format(self._conn)

    def close(self):
        self._conn.close()

    @asyncio.coroutine
    def wait_closed(self):
        yield from self._conn.wait_closed()

    @property
    def db(self):
        """Currently selected db index."""
        return self._conn.db

    @property
    def encoding(self):
        """Current set codec or None."""
        return self._conn.encoding

    @property
    def connection(self):
        """:class:`aioredis.RedisConnection` instance."""
        return self._conn

    @property
    def in_transaction(self):
        """Set to True when MULTI command was issued."""
        return self._conn.in_transaction

    @property
    def closed(self):
        """True if connection is closed."""
        return self._conn.closed

    def auth(self, password):
        """Authenticate to server.

        This method wraps call to :meth:`aioredis.RedisConnection.auth()`
        """
        return self._conn.auth(password)

    def echo(self, message, *, encoding=_NOTSET):
        """Echo the given string."""
        return self._conn.execute('ECHO', message, encoding=encoding)

    def ping(self, *, encoding=_NOTSET):
        """Ping the server."""
        return self._conn.execute('PING', encoding=encoding)

    def quit(self):
        """Close the connection."""
        return self._conn.execute('QUIT')

    def select(self, db):
        """Change the selected database for the current connection.

        This method wraps call to :meth:`aioredis.RedisConnection.select()`
        """
        return self._conn.select(db)


@asyncio.coroutine
def create_redis(address, *, db=None, password=None, ssl=None,
                 encoding=None, commands_factory=Redis,
                 timeout=None,
                 loop=None):
    """Creates high-level Redis interface.

    This function is a coroutine.
    """
    conn = yield from create_connection(address, db=db,
                                        password=password,
                                        ssl=ssl,
                                        encoding=encoding,
                                        timeout=timeout,
                                        loop=loop)
    return commands_factory(conn)


@asyncio.coroutine
def create_reconnecting_redis(address, *, db=None, password=None, ssl=None,
                              encoding=None, commands_factory=Redis,
                              loop=None):
    """Creates high-level Redis interface.

    This function is a coroutine.
    """
    # Note: this is not coroutine, but we may make it such. We may start
    # a first connection in it, or just resolve DNS. So let's keep it
    # coroutine for forward compatibility
    conn = AutoConnector(address,
                         db=db, password=password, ssl=ssl,
                         encoding=encoding, loop=loop)
    return commands_factory(conn)


# make pyflakes happy
(Pipeline, MultiExec)
