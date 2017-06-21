import asyncio
import collections
import sys
import warnings
import types

from .connection import create_connection, _PUBSUB_COMMANDS
from .log import logger
from .util import async_task, _NOTSET
from .errors import PoolClosedError
from .abc import AbcPool
from .locks import Lock


PY_35 = sys.version_info >= (3, 5)


@asyncio.coroutine
def create_pool(address, *, db=None, password=None, ssl=None, encoding=None,
                minsize=1, maxsize=10, commands_factory=_NOTSET,
                parser=None, loop=None, create_connection_timeout=None,
                pool_cls=None, connection_cls=None):
    # FIXME: rewrite docstring
    """Creates Redis Pool.

    By default it creates pool of Redis instances, but it is
    also possible to create pool of plain connections by passing
    ``lambda conn: conn`` as commands_factory.

    *commands_factory* parameter is deprecated since v0.2.9

    All arguments are the same as for create_connection.

    Returns RedisPool instance or a pool_cls if it is given.
    """
    if commands_factory is not _NOTSET:
        warnings.warn(
            "commands_factory argument is deprecated and will be removed!",
            DeprecationWarning)

    if pool_cls:
        assert issubclass(pool_cls, AbcPool),\
                "pool_class does not meet the AbcPool contract"
        cls = pool_cls
    else:
        cls = ConnectionsPool

    pool = cls(address, db, password, encoding,
               minsize=minsize, maxsize=maxsize,
               ssl=ssl, parser=parser,
               create_connection_timeout=create_connection_timeout,
               connection_cls=connection_cls,
               loop=loop)
    try:
        yield from pool._fill_free(override_min=False)
    except Exception as ex:
        pool.close()
        yield from pool.wait_closed()
        raise
    return pool


class ConnectionsPool(AbcPool):
    """Redis connections pool."""

    def __init__(self, address, db=None, password=None, encoding=None,
                 *, minsize, maxsize, ssl=None, parser=None,
                 create_connection_timeout=None,
                 connection_cls=None,
                 loop=None):
        assert isinstance(minsize, int) and minsize >= 0, (
            "minsize must be int >= 0", minsize, type(minsize))
        assert maxsize is not None, "Arbitrary pool size is disallowed."
        assert isinstance(maxsize, int) and maxsize > 0, (
            "maxsize must be int > 0", maxsize, type(maxsize))
        assert minsize <= maxsize, (
            "Invalid pool min/max sizes", minsize, maxsize)
        if loop is None:
            loop = asyncio.get_event_loop()
        self._address = address
        self._db = db
        self._password = password
        self._ssl = ssl
        self._encoding = encoding
        self._parser_class = parser
        self._minsize = minsize
        self._create_connection_timeout = create_connection_timeout
        self._loop = loop
        self._pool = collections.deque(maxlen=maxsize)
        self._used = set()
        self._acquiring = 0
        self._cond = asyncio.Condition(lock=Lock(loop=loop), loop=loop)
        self._close_state = asyncio.Event(loop=loop)
        self._close_waiter = None
        self._pubsub_conn = None
        self._connection_cls = connection_cls

    def __repr__(self):
        return '<{} [db:{}, size:[{}:{}], free:{}]>'.format(
            self.__class__.__name__, self.db,
            self.minsize, self.maxsize, self.freesize)

    @property
    def minsize(self):
        """Minimum pool size."""
        return self._minsize

    @property
    def maxsize(self):
        """Maximum pool size."""
        return self._pool.maxlen

    @property
    def size(self):
        """Current pool size."""
        return self.freesize + len(self._used) + self._acquiring

    @property
    def freesize(self):
        """Current number of free connections."""
        return len(self._pool)

    @property
    def address(self):
        return self._address

    @asyncio.coroutine
    def clear(self):
        """Clear pool connections.

        Close and remove all free connections.
        """
        with (yield from self._cond):
            yield from self._do_clear()

    @asyncio.coroutine
    def _do_clear(self):
        waiters = []
        while self._pool:
            conn = self._pool.popleft()
            conn.close()
            waiters.append(conn.wait_closed())
        yield from asyncio.gather(*waiters, loop=self._loop)

    @asyncio.coroutine
    def _do_close(self):
        yield from self._close_state.wait()
        with (yield from self._cond):
            assert not self._acquiring, self._acquiring
            waiters = []
            while self._pool:
                conn = self._pool.popleft()
                conn.close()
                waiters.append(conn.wait_closed())
            for conn in self._used:
                conn.close()
                waiters.append(conn.wait_closed())
            yield from asyncio.gather(*waiters, loop=self._loop)
            # TODO: close _pubsub_conn connection
            logger.debug("Closed %d connection(s)", len(waiters))

    def close(self):
        """Close all free and in-progress connections and mark pool as closed.
        """
        if not self._close_state.is_set():
            self._close_waiter = async_task(self._do_close(), loop=self._loop)
            self._close_state.set()

    @property
    def closed(self):
        """True if pool is closed."""
        return self._close_state.is_set()

    @asyncio.coroutine
    def wait_closed(self):
        """Wait until pool gets closed."""
        yield from self._close_state.wait()
        assert self._close_waiter is not None
        yield from asyncio.shield(self._close_waiter, loop=self._loop)

    @property
    def db(self):
        """Currently selected db index."""
        return self._db or 0

    @property
    def encoding(self):
        """Current set codec or None."""
        return self._encoding

    def execute(self, command, *args, **kw):
        """Executes redis command in a free connection and returns
        future waiting for result.

        Picks connection from free pool and send command through
        that connection.
        If no connection is found, returns coroutine waiting for
        free connection to execute command.
        """
        conn, address = self.get_connection(command, args)
        if conn is not None:
            fut = conn.execute(command, *args, **kw)
            return self._check_result(fut, command, args, kw)
        else:
            coro = self._wait_execute(address, command, args, kw)
            return self._check_result(coro, command, args, kw)

    def execute_pubsub(self, command, *channels):
        """Executes Redis (p)subscribe/(p)unsubscribe commands.

        ConnectionsPool picks separate connection for pub/sub
        and uses it until explicitly closed or disconnected
        (unsubscribing from all channels/patterns will leave connection
         locked for pub/sub use).

        There is no auto-reconnect for this PUB/SUB connection.

        Returns asyncio.gather coroutine waiting for all channels/patterns
        to receive answers.
        """
        conn, address = self.get_connection(command)
        if conn is not None:
            return conn.execute_pubsub(command, *channels)
        else:
            return self._wait_execute_pubsub(address, command, channels, {})

    def get_connection(self, command, args=()):
        """Get free connection from pool.

        Returns connection.
        """
        # TODO: find a better way to determine if connection is free
        #       and not havily used.
        command = command.upper().strip()
        is_pubsub = command in _PUBSUB_COMMANDS
        if is_pubsub and self._pubsub_conn:
            if not self._pubsub_conn.closed:
                return self._pubsub_conn, self._pubsub_conn.address
            self._pubsub_conn = None
        for i in range(self.freesize):
            conn = self._pool[0]
            self._pool.rotate(1)
            if conn.closed:  # or conn._waiters: (eg: busy connection)
                continue
            if conn.in_pubsub:
                continue
            if is_pubsub:
                self._pubsub_conn = conn
                self._pool.remove(conn)
                self._used.add(conn)
            return conn, conn.address
        return None, self._address  # figure out

    def _check_result(self, fut, *data):
        """Hook to check result or catch exception (like MovedError).

        This method can be coroutine.
        """
        return fut

    @asyncio.coroutine
    def _wait_execute(self, address, command, args, kw):
        """Acquire connection and execute command."""
        conn = yield from self.acquire(command, args)
        try:
            return (yield from conn.execute(command, *args, **kw))
        finally:
            self.release(conn)

    @asyncio.coroutine
    def _wait_execute_pubsub(self, address, command, args, kw):
        if self.closed:
            raise PoolClosedError("Pool is closed")
        assert self._pubsub_conn is None or self._pubsub_conn.closed, (
            "Expected no or closed connection", self._pubsub_conn)
        with (yield from self._cond):
            if self.closed:
                raise PoolClosedError("Pool is closed")
            if self._pubsub_conn is None or self._pubsub_conn.closed:
                conn = yield from self._create_new_connection(address)
                self._pubsub_conn = conn
            conn = self._pubsub_conn
            return (yield from conn.execute_pubsub(command, *args, **kw))

    @asyncio.coroutine
    def select(self, db):
        """Changes db index for all free connections.

        All previously acquired connections will be closed when released.
        """
        res = True
        with (yield from self._cond):
            for i in range(self.freesize):
                res = res and (yield from self._pool[i].select(db))
            else:
                self._db = db
        return res

    @asyncio.coroutine
    def auth(self, password):
        self._password = password
        with (yield from self._cond):
            for i in range(self.freesize):
                yield from self._pool[i].auth(password)

    @property
    def in_pubsub(self):
        if self._pubsub_conn and not self._pubsub_conn.closed:
            return self._pubsub_conn.in_pubsub
        return 0

    @property
    def pubsub_channels(self):
        if self._pubsub_conn and not self._pubsub_conn.closed:
            return self._pubsub_conn.pubsub_channels
        return types.MappingProxyType({})

    @property
    def pubsub_patterns(self):
        if self._pubsub_conn and not self._pubsub_conn.closed:
            return self._pubsub_conn.pubsub_patterns
        return types.MappingProxyType({})

    @asyncio.coroutine
    def acquire(self, command=None, args=()):
        """Acquires a connection from free pool.

        Creates new connection if needed.
        """
        if self.closed:
            raise PoolClosedError("Pool is closed")
        with (yield from self._cond):
            if self.closed:
                raise PoolClosedError("Pool is closed")
            while True:
                yield from self._fill_free(override_min=True)
                if self.freesize:
                    conn = self._pool.popleft()
                    assert not conn.closed, conn
                    assert conn not in self._used, (conn, self._used)
                    self._used.add(conn)
                    return conn
                else:
                    yield from self._cond.wait()

    def release(self, conn):
        """Returns used connection back into pool.

        When returned connection has db index that differs from one in pool
        the connection will be closed and dropped.
        When queue of free connections is full the connection will be dropped.
        """
        assert conn in self._used, (
            "Invalid connection, maybe from other pool", conn)
        self._used.remove(conn)
        if not conn.closed:
            if conn.in_transaction:
                logger.warning(
                    "Connection %r is in transaction, closing it.", conn)
                conn.close()
            elif conn.in_pubsub:
                logger.warning(
                    "Connection %r is in subscribe mode, closing it.", conn)
                conn.close()
            elif conn._waiters:
                logger.warning(
                    "Connection %r has pending commands, closing it.", conn)
                conn.close()
            elif conn.db == self.db:
                if self.maxsize and self.freesize < self.maxsize:
                    self._pool.append(conn)
                else:
                    # consider this connection as old and close it.
                    conn.close()
            else:
                conn.close()
        # FIXME: check event loop is not closed
        async_task(self._wakeup(), loop=self._loop)

    def _drop_closed(self):
        for i in range(self.freesize):
            conn = self._pool[0]
            if conn.closed:
                self._pool.popleft()
            else:
                self._pool.rotate(1)

    @asyncio.coroutine
    def _fill_free(self, *, override_min):
        # drop closed connections first
        self._drop_closed()
        address = self._address
        while self.size < self.minsize:
            self._acquiring += 1
            try:
                conn = yield from self._create_new_connection(address)
                self._pool.append(conn)
            finally:
                self._acquiring -= 1
                # connection may be closed at yield point
                self._drop_closed()
        if self.freesize:
            return
        if override_min:
            while not self._pool and self.size < self.maxsize:
                self._acquiring += 1
                try:
                    conn = yield from self._create_new_connection(address)
                    self._pool.append(conn)
                finally:
                    self._acquiring -= 1
                    # connection may be closed at yield point
                    self._drop_closed()

    def _create_new_connection(self, address):
        return create_connection(address,
                                 db=self._db,
                                 password=self._password,
                                 ssl=self._ssl,
                                 encoding=self._encoding,
                                 parser=self._parser_class,
                                 timeout=self._create_connection_timeout,
                                 connection_cls=self._connection_cls,
                                 loop=self._loop)

    @asyncio.coroutine
    def _wakeup(self, closing_conn=None):
        with (yield from self._cond):
            self._cond.notify()
        if closing_conn is not None:
            yield from closing_conn.wait_closed()

    def __enter__(self):
        raise RuntimeError(
            "'yield from' should be used as a context manager expression")

    def __exit__(self, *args):
        pass    # pragma: nocover

    def __iter__(self):
        # this method is needed to allow `yield`ing from pool
        conn = yield from self.acquire()
        return _ConnectionContextManager(self, conn)

    if PY_35:
        def __await__(self):
            # To make `with await pool` work
            conn = yield from self.acquire()
            return _ConnectionContextManager(self, conn)

        def get(self):
            '''Return async context manager for working with connection.

            async with pool.get() as conn:
                await conn.get(key)
            '''
            return _AsyncConnectionContextManager(self)


class _ConnectionContextManager:

    __slots__ = ('_pool', '_conn')

    def __init__(self, pool, conn):
        self._pool = pool
        self._conn = conn

    def __enter__(self):
        return self._conn

    def __exit__(self, exc_type, exc_value, tb):
        try:
            self._pool.release(self._conn)
        finally:
            self._pool = None
            self._conn = None


if PY_35:
    class _AsyncConnectionContextManager:

        __slots__ = ('_pool', '_conn')

        def __init__(self, pool):
            self._pool = pool
            self._conn = None

        @asyncio.coroutine
        def __aenter__(self):
            conn = yield from self._pool.acquire()
            self._conn = conn
            return self._conn

        @asyncio.coroutine
        def __aexit__(self, exc_type, exc_value, tb):
            try:
                self._pool.release(self._conn)
            finally:
                self._pool = None
                self._conn = None
