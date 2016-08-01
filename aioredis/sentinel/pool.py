# Implement Sentinel connections pool.
#
# It must provide methods for getting client pools (to master/slave)
# and control them.
#
# MasterConnectionsPool:
#   will keep a link to sentinel/parent pool
#   on connection close -- must trigger sentinel to
#       rediscover and reconnect all redises;

# NOTE: need some benchmarks to test python code/abstractions speed;

# TODO:
#   1. Generalize Pool API (BaseConnectionsPool?);
#   2. Generalize Connection API (BaseConnection?);
#   3. Use Pool in ConnectionsPool;
#   4. Make MastersPool (reference to sentinels);
#   5. Measure python abstractions overhead (isinstance/getattr/etc)

import asyncio
from concurrent.futures import ALL_COMPLETED

from ..log import sentinel_logger
from ..util import async_task, Channel
from ..pool import create_pool, ConnectionsPool


_NON_DISCOVERED = object()


@asyncio.coroutine
def create_sentinel_pool(sentinels, *, db=None, password=None,
                         encoding=None, minsize=1, maxsize=10,
                         ssl=None, loop=None):
    """Create SentinelPool."""
    assert isinstance(sentinels, (list, tuple)), sentinels
    if loop is None:
        loop = asyncio.get_event_loop()

    pool = SentinelPool(sentinels, db=db,
                        password=password,
                        ssl=ssl,
                        encoding=encoding,
                        minsize=minsize,
                        maxsize=maxsize,
                        loop=loop)
    yield from pool.discover()
    return pool


class ManagedPool(ConnectionsPool):

    def __init__(self, sentinel, service, is_master,
                 db=None, password=None, encoding=None,
                 *, minsize, maxsize, ssl=None, loop=None):
        super().__init__(_NON_DISCOVERED,
                         db=db, password=password, encoding=encoding,
                         minsize=minsize, maxsize=maxsize, ssl=ssl, loop=loop)
        assert self._address is _NON_DISCOVERED
        self._sentinel = sentinel
        self._service = service
        self._is_master = is_master

    @property
    def address(self):
        if self._address is _NON_DISCOVERED:
            return
        return self._address

    @asyncio.coroutine
    def _create_new_connection(self, address):
        if address is _NON_DISCOVERED:
            # cache service address
            if self._is_master:
                cmd = b'GET-MASTER-ADDR-BY-NAME'
            else:
                cmd = b'SLAVES'
            yield from self._do_clear()
            host, port = yield from self._sentinel.execute(
                b'SENTINEL', cmd, self._service)
            address = host, int(port)
            self._address = address
            sentinel_logger.debug("Discoverred new address %r for %s",
                                  address, self._service)
        return (yield from super()._create_new_connection(address))

    def _drop_closed(self):
        diff = len(self._pool)
        super()._drop_closed()
        diff = diff - len(self._pool)
        if diff:
            # if closed connections in pool: reset addr; notify sentinel
            sentinel_logger.debug(
                "Dropped %d closed connnection(s); must rediscover", diff)
            self._sentinel._rediscover(self._service)

    def release(self, conn):
        was_closed = conn.closed
        super().release(conn)
        # if connection was closed in use and not by release()
        if was_closed:
            sentinel_logger.debug(
                "Released closed connection; must rediscover")
            self._sentinel._rediscover(self._service)

    def need_rediscover(self):
        self._address = _NON_DISCOVERED


class SentinelPool:  # TODO: implement AbcPool?

    def __init__(self, sentinels, *, db=None, password=None, ssl=None,
                 encoding=None, minsize, maxsize, loop=None):
        # TODO: add redis init parameters (db, auth, pool size)
        if loop is None:
            loop = asyncio.get_event_loop()
        # XXX: _sentinels is unordered
        self._sentinels = set(sentinels)
        self._loop = loop
        self._pools = []     # list of sentinel pools
        self._masters = {}
        self._slaves = {}
        self._redis_db = db
        self._redis_password = password
        self._redis_ssl = ssl
        self._redis_encoding = encoding
        self._redis_minsize = minsize
        self._redis_maxsize = maxsize
        self._close_state = asyncio.Event(loop=loop)
        self._close_waiter = async_task(self._do_close(), loop=loop)

    def get_master(self, service):
        """Returns wrapper to master's pool for requested service."""
        # TODO: make it coroutine and connect minsize connections
        if service not in self._masters:
            self._masters[service] = ManagedPool(
                self, service, is_master=True,
                db=self._redis_db,
                password=self._redis_password,
                encoding=self._redis_encoding,
                minsize=self._redis_minsize,
                maxsize=self._redis_maxsize,
                ssl=self._redis_ssl,
                loop=self._loop)
        return self._masters[service]

    def get_slave(self, service):
        """Returns wrapper to slave's pool for requested service."""
        # TODO: make it coroutine and connect minsize connections
        if service not in self._slaves:
            self._slaves[service] = ManagedPool(
                self, service, is_master=False,
                db=self._redis_db,
                password=self._redis_password,
                encoding=self._redis_encoding,
                minsize=self._redis_minsize,
                maxsize=self._redis_maxsize,
                ssl=self._redis_ssl,
                loop=self._loop)
        return self._slaves[service]

    def execute(self, command, *args, **kwargs):
        """Execute sentinel command."""
        # TODO: choose pool
        #   kwargs can be used to control which sentinel to communicate to
        for pool in self._pools:
            return pool.execute(command, *args, **kwargs)
        # how to handle errors and pick other pool?
        #   is the only way to make it coroutine?

    def create_sentinel_pool(self, address):
        """Create connections pool to Sentinel instance."""
        # We create connections pool to sentinel with 2 connections maximum
        #   one for control commands and one for pub/sub.
        # We use pool for reconnections
        # TODO: configure sentinel's pool
        return create_pool(address, minsize=1, maxsize=2,
                           encoding='utf-8', loop=self._loop)

    @asyncio.coroutine
    def discover(self, timeout=0.2):    # TODO: better name?
        """Discover sentinels and all monitored services within given timeout.

        If no sentinels discovered within timeout: TimeoutError is raised.
        If some sentinels were discovered but not all — it is ok.
        If not all monitored services (masters/slaves) discovered
        (or connections established) — it is ok.
        TBD: what if some sentinels/services unreachable;
        """
        # TODO: check not closed
        # TODO: discovery must be done with some customizable timeout.
        tasks = []

        for addr in self._sentinels:    # iterate over unordered set
            tasks.append(self._connect_sentinel(addr, timeout))
        done, pending = yield from asyncio.wait(tasks, loop=self._loop,
                                                return_when=ALL_COMPLETED)
        assert not pending, ("Expected all tasks to complete", done, pending)
        pools = []

        for task in done:
            result = task.result()
            if isinstance(result, Exception):
                pass    # FIXME
            else:
                pools.append(result)
        if not pools:
            raise Exception("Could not connect to any sentinel")
        pools, self._pools[:] = self._pools[:], pools
        # TODO: close current connections
        for pool in pools:
            pool.close()
            yield from pool.wait_closed()

    @asyncio.coroutine
    def _connect_sentinel(self, address, timeout):
        """Try to connect to specified Sentinel returning either
        connections pool or exception.
        """
        try:
            pool = yield from asyncio.wait_for(
                self.create_sentinel_pool(address),
                timeout=timeout, loop=self._loop)

            ch = Channel('*', is_pattern=True, loop=self._loop)
            yield from pool.execute_pubsub('psubscribe', ch)

            @asyncio.coroutine
            def echo_events(ch):
                while (yield from ch.wait_message()):
                    msg = yield from ch.get(encoding='utf-8')
                    sentinel_logger.debug("Notification: %s", msg)
            asyncio.ensure_future(echo_events(ch), loop=self._loop)

            return pool
        except asyncio.TimeoutError as err:
            sentinel_logger.debug(
                "Failed to connect to Sentinel(%r) within %ss timeout",
                address, timeout)
            return err
        except Exception as err:
            sentinel_logger.debug(
                "Error connecting to Sentinel(%r): %r", address, err)
            return err

    @property
    def closed(self):
        """True if pool is closed or closing."""
        return self._close_state.is_set()

    def close(self):
        """Close all controlled connections (both sentinel and redis)."""
        if not self._close_state.is_set():
            self._close_state.set()

    @asyncio.coroutine
    def _do_close(self):
        yield from self._close_state.wait()
        # TODO: lock
        tasks = []
        while self._pools:
            pool = self._pools.pop(0)
            pool.close()
            tasks.append(pool.wait_closed())
        while self._masters:
            _, pool = self._masters.popitem()
            pool.close()
            tasks.append(pool.wait_closed())
        while self._slaves:
            _, pool = self._slaves.popitem()
            pool.close()
            tasks.append(pool.wait_closed())
        yield from asyncio.gather(*tasks, loop=self._loop)

    @asyncio.coroutine
    def wait_closed(self):
        """Wait until pool gets closed."""
        yield from asyncio.shield(self._close_waiter, loop=self._loop)

    def _rediscover(self, service):
        sentinel_logger.debug(
            "Must redisover services; service %s disconnected", service)
        for service, pool in self._masters.items():
            pool.need_rediscover()
        for service, pool in self._slaves.items():
            pool.need_rediscover()
