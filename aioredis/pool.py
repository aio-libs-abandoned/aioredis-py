import asyncio
import collections
import sys
import warnings

from .connection import create_connection
from .log import logger
from .util import async_task, _NOTSET
from .errors import PoolClosedError


PY_35 = sys.version_info >= (3, 5)


@asyncio.coroutine
def create_pool(address, *, db=0, password=None, ssl=None, encoding=None,
                minsize=1, maxsize=10, commands_factory=_NOTSET, loop=None):
    """Creates Redis Pool.

    By default it creates pool of Redis instances, but it is
    also possible to create pool of plain connections by passing
    ``lambda conn: conn`` as commands_factory.

    *commands_factory* parameter is deprecated since v0.2.9

    All arguments are the same as for create_connection.

    Returns RedisPool instance.
    """
    if commands_factory is not _NOTSET:
        warnings.warn(
            "commands_factory argument is deprecated and will be removed!",
            warnings.DeprecationWarning)

    pool = ConnectionsPool(address, db, password, encoding,
                           minsize=minsize, maxsize=maxsize,
                           ssl=ssl, loop=loop)
    try:
        yield from pool._fill_free(override_min=False)
    except Exception as ex:
        pool.close()
        yield from pool.wait_closed()
        raise
    return pool


class ConnectionsPool:
    """Redis connections pool.
    """

    def __init__(self, address, db=0, password=None, encoding=None,
                 *, minsize, maxsize, ssl=None, loop=None):
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
        self._minsize = minsize
        self._loop = loop
        self._pool = collections.deque(maxlen=maxsize)
        self._used = set()
        self._acquiring = 0
        self._cond = asyncio.Condition(loop=loop)
        self._close_state = asyncio.Event(loop=loop)
        self._close_waiter = async_task(self._do_close(), loop=loop)

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

    @asyncio.coroutine
    def clear(self):
        """Clear pool connections.

        Close and remove all free connections.
        """
        with (yield from self._cond):
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
            logger.debug("Closed %d connections", len(waiters))

    def close(self):
        """Close all free and in-progress connections and mark pool as closed.
        """
        if not self._close_state.is_set():
            self._close_state.set()

    @property
    def closed(self):
        """True if pool is closed."""
        return self._close_state.is_set()

    @asyncio.coroutine
    def wait_closed(self):
        """Wait until pool gets closed."""
        yield from asyncio.shield(self._close_waiter, loop=self._loop)

    @property
    def db(self):
        """Currently selected db index."""
        return self._db

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

    def get_connection(self, command, args):
        """Get free connection from pool.

        Returns connection and its address.
        """
        # TODO: find a better way to determine if connection is free
        #       and not havily used.
        for i in range(self.freesize):
            conn = self._pool[0]
            self._pool.rotate(1)
            if conn.closed:  # or conn._waiters:
                continue
            return conn, None
        return None, None

    def _check_result(self, fut, *data):
        """Hook to check result or catch exception (like MovedError).

        This method can be coroutine.
        """
        return fut

    @asyncio.coroutine
    def _wait_execute(self, address, command, args, kw):
        """Acquire connection and execute command."""
        conn, address = yield from self.acquire(command, args)
        try:
            return (yield from conn.execute(command, *args, **kw))
        finally:
            self.release(conn)

    @asyncio.coroutine
    def select(self, db):
        """Changes db index for all free connections.

        All previously acquired connections will be closed when released.
        """
        with (yield from self._cond):
            for i in range(self.freesize):
                yield from self._pool[i].select(db)
            else:
                self._db = db

    @asyncio.coroutine
    def acquire(self):
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
                    return conn, self._address
                else:
                    yield from self._cond.wait()

    def release(self, conn):
        """Returns used connection back into pool.

        When returned connection has db index that differs from one in pool
        the connection will be closed and dropped.
        When queue of free connections is full the connection will be dropped.
        """
        assert conn in self._used, "Invalid connection, maybe from other pool"
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
            self._conn = yield from self._pool.acquire()
            return self._conn

        @asyncio.coroutine
        def __aexit__(self, exc_type, exc_value, tb):
            try:
                self._pool.release(self._conn)
            finally:
                self._pool = None
                self._conn = None
