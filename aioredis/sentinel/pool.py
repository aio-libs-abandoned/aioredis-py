import asyncio
import contextlib

from concurrent.futures import ALL_COMPLETED
from async_timeout import timeout as async_timeout

from ..log import sentinel_logger
from ..pubsub import Receiver
from ..pool import create_pool, ConnectionsPool
from ..errors import (
    MasterNotFoundError,
    SlaveNotFoundError,
    PoolClosedError,
    RedisError,
    MasterReplyError,
    SlaveReplyError,
)


# Address marker for discovery
_NON_DISCOVERED = object()

_logger = sentinel_logger.getChild('monitor')


async def create_sentinel_pool(sentinels, *, db=None, password=None,
                               encoding=None, minsize=1, maxsize=10,
                               ssl=None, parser=None, timeout=0.2, loop=None):
    """Create SentinelPool."""
    # FIXME: revise default timeout value
    assert isinstance(sentinels, (list, tuple)), sentinels
    if loop is None:
        loop = asyncio.get_event_loop()

    pool = SentinelPool(sentinels, db=db,
                        password=password,
                        ssl=ssl,
                        encoding=encoding,
                        parser=parser,
                        minsize=minsize,
                        maxsize=maxsize,
                        timeout=timeout,
                        loop=loop)
    await pool.discover()
    return pool


class SentinelPool:
    """Sentinel connections pool.

    Holds connection pools to known and discovered (TBD) Sentinels
    as well as services' connections.
    """

    def __init__(self, sentinels, *, db=None, password=None, ssl=None,
                 encoding=None, parser=None, minsize, maxsize, timeout,
                 loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        # TODO: add connection/discover timeouts;
        #       and what to do if no master is found:
        #       (raise error or try forever or try until timeout)

        # XXX: _sentinels is unordered
        self._sentinels = set(sentinels)
        self._loop = loop
        self._timeout = timeout
        self._pools = []     # list of sentinel pools
        self._masters = {}
        self._slaves = {}
        self._parser_class = parser
        self._redis_db = db
        self._redis_password = password
        self._redis_ssl = ssl
        self._redis_encoding = encoding
        self._redis_minsize = minsize
        self._redis_maxsize = maxsize
        self._close_state = asyncio.Event(loop=loop)
        self._close_waiter = None
        self._monitor = monitor = Receiver(loop=loop)

        async def echo_events():
            try:
                while await monitor.wait_message():
                    ch, (ev, data) = await monitor.get(encoding='utf-8')
                    ev = ev.decode('utf-8')
                    _logger.debug("%s: %s", ev, data)
                    if ev in ('+odown',):
                        typ, name, *tail = data.split(' ')
                        if typ == 'master':
                            self._need_rediscover(name)
                # TODO: parse messages;
                #   watch +new-epoch which signals `failover in progres`
                #   freeze reconnection
                #   wait / discover new master (find proper way)
                #   unfreeze reconnection
                #
                #   discover master in default way
                #       get-master-addr...
                #       connnect
                #       role
                #       etc...
            except asyncio.CancelledError:
                pass
        self._monitor_task = asyncio.ensure_future(echo_events(), loop=loop)

    @property
    def discover_timeout(self):
        """Timeout (seconds) for Redis/Sentinel command calls during
        master/slave address discovery.
        """
        return self._timeout

    def master_for(self, service):
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
                parser=self._parser_class,
                loop=self._loop)
        return self._masters[service]

    def slave_for(self, service):
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
                parser=self._parser_class,
                loop=self._loop)
        return self._slaves[service]

    def execute(self, command, *args, **kwargs):
        """Execute sentinel command."""
        # TODO: choose pool
        #   kwargs can be used to control which sentinel to use
        if self.closed:
            raise PoolClosedError("Sentinel pool is closed")
        for pool in self._pools:
            return pool.execute(command, *args, **kwargs)
        # how to handle errors and pick other pool?
        #   is the only way to make it coroutine?

    @property
    def closed(self):
        """True if pool is closed or closing."""
        return self._close_state.is_set()

    def close(self):
        """Close all controlled connections (both sentinel and redis)."""
        if not self._close_state.is_set():
            self._close_waiter = asyncio.ensure_future(self._do_close(),
                                                       loop=self._loop)
            self._close_state.set()

    async def _do_close(self):
        await self._close_state.wait()
        # TODO: lock
        tasks = []
        task, self._monitor_task = self._monitor_task, None
        task.cancel()
        tasks.append(task)
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
        await asyncio.gather(*tasks, loop=self._loop)

    async def wait_closed(self):
        """Wait until pool gets closed."""
        await self._close_state.wait()
        assert self._close_waiter is not None
        await asyncio.shield(self._close_waiter, loop=self._loop)

    async def discover(self, timeout=0.2):    # TODO: better name?
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
        pools = []
        for addr in self._sentinels:    # iterate over unordered set
            tasks.append(self._connect_sentinel(addr, timeout, pools))
        done, pending = await asyncio.wait(tasks, loop=self._loop,
                                           return_when=ALL_COMPLETED)
        assert not pending, ("Expected all tasks to complete", done, pending)

        for task in done:
            result = task.result()
            if isinstance(result, Exception):
                continue    # FIXME
        if not pools:
            raise Exception("Could not connect to any sentinel")
        pools, self._pools[:] = self._pools[:], pools
        # TODO: close current connections
        for pool in pools:
            pool.close()
            await pool.wait_closed()

        # TODO: discover peer sentinels
        for pool in self._pools:
            await pool.execute_pubsub(
                b'psubscribe', self._monitor.pattern('*'))

    async def _connect_sentinel(self, address, timeout, pools):
        """Try to connect to specified Sentinel returning either
        connections pool or exception.
        """
        try:
            with async_timeout(timeout, loop=self._loop):
                pool = await create_pool(
                    address, minsize=1, maxsize=2,
                    parser=self._parser_class,
                    loop=self._loop)
            pools.append(pool)
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

    async def discover_master(self, service, timeout):
        """Perform Master discovery for specified service."""
        # TODO: get lock
        idle_timeout = timeout
        # FIXME: single timeout used 4 times;
        #   meaning discovery can take up to:
        #   3 * timeout * (sentinels count)
        #
        #   having one global timeout also can leed to
        #   a problem when not all sentinels are checked.

        # use a copy, cause pools can change
        pools = self._pools[:]
        for sentinel in pools:
            try:
                with async_timeout(timeout, loop=self._loop):
                    address = await self._get_masters_address(
                        sentinel, service)

                pool = self._masters[service]
                with async_timeout(timeout, loop=self._loop), \
                        contextlib.ExitStack() as stack:
                    conn = await pool._create_new_connection(address)
                    stack.callback(conn.close)
                    await self._verify_service_role(conn, 'master')
                    stack.pop_all()

                return conn
            except asyncio.CancelledError:
                # we must correctly handle CancelledError(s):
                #   application may be stopped or function can be cancelled
                #   by outer timeout, so we must stop the look up.
                raise
            except asyncio.TimeoutError:
                continue
            except DiscoverError as err:
                sentinel_logger.debug("DiscoverError(%r, %s): %r",
                                      sentinel, service, err)
                await asyncio.sleep(idle_timeout, loop=self._loop)
                continue
            except RedisError as err:
                raise MasterReplyError("Service {} error".format(service), err)
            except Exception:
                # TODO: clear (drop) connections to schedule reconnect
                await asyncio.sleep(idle_timeout, loop=self._loop)
                continue
        else:
            raise MasterNotFoundError("No master found for {}".format(service))

    async def discover_slave(self, service, timeout, **kwargs):
        """Perform Slave discovery for specified service."""
        # TODO: use kwargs to change how slaves are picked up
        #   (eg: round-robin, priority, random, etc)
        idle_timeout = timeout
        pools = self._pools[:]
        for sentinel in pools:
            try:
                with async_timeout(timeout, loop=self._loop):
                    address = await self._get_slave_address(
                        sentinel, service)  # add **kwargs
                pool = self._slaves[service]
                with async_timeout(timeout, loop=self._loop), \
                        contextlib.ExitStack() as stack:
                    conn = await pool._create_new_connection(address)
                    stack.callback(conn.close)
                    await self._verify_service_role(conn, 'slave')
                    stack.pop_all()
                return conn
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                continue
            except DiscoverError:
                await asyncio.sleep(idle_timeout, loop=self._loop)
                continue
            except RedisError as err:
                raise SlaveReplyError("Service {} error".format(service), err)
            except Exception:
                await asyncio.sleep(idle_timeout, loop=self._loop)
                continue
        raise SlaveNotFoundError("No slave found for {}".format(service))

    async def _get_masters_address(self, sentinel, service):
        # NOTE: we don't use `get-master-addr-by-name`
        #   as it can provide stale data so we repeat
        #   after redis-py and check service flags.
        state = await sentinel.execute(b'sentinel', b'master',
                                       service, encoding='utf-8')
        if not state:
            raise UnknownService()
        state = make_dict(state)
        address = state['ip'], int(state['port'])
        flags = set(state['flags'].split(','))
        if {'s_down', 'o_down', 'disconnected'} & flags:
            raise BadState(state)
        return address

    async def _get_slave_address(self, sentinel, service):
        # Find and return single slave address
        slaves = await sentinel.execute(b'sentinel', b'slaves',
                                        service, encoding='utf-8')
        if not slaves:
            raise UnknownService()
        for state in map(make_dict, slaves):
            address = state['ip'], int(state['port'])
            flags = set(state['flags'].split(','))
            if {'s_down', 'o_down', 'disconnected'} & flags:
                continue
            return address
        else:
            raise BadState(state)   # XXX: only last state

    async def _verify_service_role(self, conn, role):
        res = await conn.execute(b'role', encoding='utf-8')
        if res[0] != role:
            raise RoleMismatch(res)

    def _need_rediscover(self, service):
        sentinel_logger.debug("Must redisover service %s", service)
        for service, pool in self._masters.items():
            pool.need_rediscover()
        for service, pool in self._slaves.items():
            pool.need_rediscover()


class ManagedPool(ConnectionsPool):

    def __init__(self, sentinel, service, is_master,
                 db=None, password=None, encoding=None, parser=None,
                 *, minsize, maxsize, ssl=None, loop=None):
        super().__init__(_NON_DISCOVERED,
                         db=db, password=password, encoding=encoding,
                         minsize=minsize, maxsize=maxsize, ssl=ssl,
                         parser=parser, loop=loop)
        assert self._address is _NON_DISCOVERED
        self._sentinel = sentinel
        self._service = service
        self._is_master = is_master
        self._discover_timeout = .2

    @property
    def address(self):
        if self._address is _NON_DISCOVERED:
            return
        return self._address

    def get_connection(self, command, args=()):
        if self._address is _NON_DISCOVERED:
            return None, _NON_DISCOVERED
        return super().get_connection(command, args)

    async def _create_new_connection(self, address):
        if address is _NON_DISCOVERED:
            # Perform service discovery.
            # Returns Connection or raises error if no service can be found.
            await self._do_clear()  # make `clear` blocking

            if self._is_master:
                conn = await self._sentinel.discover_master(
                    self._service, timeout=self._sentinel.discover_timeout)
            else:
                conn = await self._sentinel.discover_slave(
                    self._service, timeout=self._sentinel.discover_timeout)
            self._address = conn.address
            sentinel_logger.debug("Discoverred new address %r for %s",
                                  conn.address, self._service)
            return conn
        return await super()._create_new_connection(address)

    def _drop_closed(self):
        diff = len(self._pool)
        super()._drop_closed()
        diff -= len(self._pool)
        if diff:
            # closed connections were in pool:
            #   * reset address;
            #   * notify sentinel pool
            sentinel_logger.debug(
                "Dropped %d closed connnection(s); must rediscover", diff)
            self._sentinel._need_rediscover(self._service)

    async def acquire(self, command=None, args=()):
        if self._address is _NON_DISCOVERED:
            await self.clear()
        return await super().acquire(command, args)

    def release(self, conn):
        was_closed = conn.closed
        super().release(conn)
        # if connection was closed while used and not by release()
        if was_closed:
            sentinel_logger.debug(
                "Released closed connection; must rediscover")
            self._sentinel._need_rediscover(self._service)

    def need_rediscover(self):
        self._address = _NON_DISCOVERED


def make_dict(plain_list):
    it = iter(plain_list)
    return dict(zip(it, it))


class DiscoverError(Exception):
    """Internal errors for masters/slaves discovery."""


class BadState(DiscoverError):
    """Bad master's / slave's state read from sentinel."""


class UnknownService(DiscoverError):
    """Service is not monitored by specific sentinel."""


class RoleMismatch(DiscoverError):
    """Service reported to have other Role."""
