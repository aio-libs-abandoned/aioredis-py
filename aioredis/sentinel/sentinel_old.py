import asyncio

from .util import coerced_keys_dict
from .connection import create_connection
from .errors import MasterNotFoundError, SlaveNotFoundError, RedisError, \
    ReadOnlyError
from .commands import create_redis, Redis


class SentinelManagedConnection(object):
    closed = False

    def __init__(self, sentinel_service, **conn_kwargs):
        self._sentinel_service = sentinel_service
        self._conn_kwargs = conn_kwargs
        self._conn = None
        self._loop = conn_kwargs.get('loop')
        if not self._loop:
            self._loop = asyncio.get_event_loop()
        self._lock = asyncio.Lock(loop=self._loop)

    def close(self):
        if self._conn is not None:
            self._conn.close()

    @property
    def in_transaction(self):
        if self._conn is None:
            return False
        # Set to True when MULTI command was issued.
        return self._conn.in_transaction

    @property
    def in_pubsub(self):
        """Indicates that connection is in PUB/SUB mode.

        Provides the number of subscribed channels.
        """
        if self._conn is None:
            return False
        return self._conn.in_pubsub

    @property
    def pubsub_channels(self):
        """Returns read-only channels dict."""
        if self._conn is None:
            return coerced_keys_dict()
        return self._conn.pubsub_channels

    @property
    def pubsub_patterns(self):
        """Returns read-only patterns dict."""
        if self._conn is None:
            return coerced_keys_dict()
        return self._conn.pubsub_patterns

    @asyncio.coroutine
    def auth(self, password):
        if self._conn is None:
            self._conn = yield from self.get_atomic_connection()
        return self._conn.auth(password)

    @asyncio.coroutine
    def wait_closed(self):
        if self._conn is not None:
            yield from self._conn.wait_closed()

    @asyncio.coroutine
    def _execute(self, *args, pub_sub=False, **kwargs):
        first_time = True

        while True:
            conn = yield from self.get_atomic_connection()
            try:
                if not pub_sub:
                    resp = yield from conn.execute(*args, **kwargs)
                else:
                    resp = yield from conn.execute_pubsub(*args, **kwargs)
                return resp
            except ReadOnlyError:
                if self._sentinel_service.is_master:
                    # When talking to a master, a ReadOnlyError when likely
                    # indicates that the previous master that we're still
                    # connected to has been demoted to a slave and there's
                    # a new master.
                    # calling disconnect will force the connection to re-query
                    # sentinel during the next connect() attempt.
                    self._conn.close()
                    yield from self._conn.wait_closed()
                    if first_time:
                        first_time = False
                        continue
                    raise ConnectionError('The previous master is now a slave')
                raise

    def execute(self, *args, **kwargs):
        return self._execute(*args, pub_sub=False, **kwargs)

    def execute_pubsub(self, *args, **kwargs):
        return self._execute(*args, pub_sub=True, **kwargs)

    @asyncio.coroutine
    def get_atomic_connection(self):
        if self._conn is None or self._conn.closed:
            with (yield from self._lock):
                if self._conn is None or self._conn.closed:
                    if self._sentinel_service.is_master:
                        service = self._sentinel_service
                        addr = yield from service.get_master_address()
                        conn = yield from create_connection(
                            addr, **self._conn_kwargs)
                        self._conn = conn
                    else:
                        service = self._sentinel_service
                        slaves = yield from service.get_slaves()
                        num = service.num_slaves()
                        for idx in range(num):
                            slave = slaves[idx]
                            try:
                                conn = yield from create_connection(
                                    slave, **self._conn_kwargs)
                                self._conn = conn
                                return self._conn
                            except SlaveNotFoundError:
                                raise
                            except RedisError:
                                pass
                        addr = yield from service.get_master_address()
                        try:
                            conn = yield from create_connection(
                                addr, **self._conn_kwargs)
                            self._conn = conn
                            return self._conn
                        except RedisError:
                            raise SlaveNotFoundError
        return self._conn


@asyncio.coroutine
def create_sentinel_connection(sentinel_service, loop=None,
                               **connection_kwargs):
    conn = SentinelManagedConnection(sentinel_service, loop=loop,
                                     **connection_kwargs)
    ret = Redis(conn)
    yield from conn.get_atomic_connection()
    return ret


@asyncio.coroutine
def create_sentinel(sentinels, *, db=None, password=None,
                    encoding=None, min_other_sentinels=0, loop=None):
    """Creates redis sentinel

    `sentinels`` is a list of sentinel nodes. Each node is represented by
    a pair (hostname, port).

    ``min_other_sentinels`` defined a minimum number of peers for a sentinel.
    When querying a sentinel, if it doesn't meet this threshold, responses
    from that sentinel won't be considered valid.

    Return value is RedisSentinel instance.
    """

    sentinels_connections = []
    for hostname, port in sentinels:
        sentinel = yield from create_redis((hostname, port), db=db,
                                           password=password,
                                           encoding=encoding, loop=loop)
        sentinels_connections.append(sentinel)
    return RedisSentinel(sentinels_connections, min_other_sentinels, loop=loop)


class RedisSentinelService:
    def __init__(self, service_name, sentinel, is_master):
        self._is_master = is_master
        self._service_name = service_name
        self._sentinel = sentinel
        self._master_address = None
        self._slaves = None

    @property
    def is_master(self):
        return self._is_master

    @asyncio.coroutine
    def get_master_address(self):
        master_address = yield from self._sentinel.discover_master(
            self._service_name)
        if self._is_master:
            if self._master_address is None:
                self._master_address = master_address
            elif master_address != self._master_address:
                # Master address changed, disconnect all clients in this pool
                # TODO
                pass
        return master_address

    def num_slaves(self):
        if self._slaves is None:
            raise RuntimeError('get_slaves coroutine should be called first')
        return len(self._slaves)

    @asyncio.coroutine
    def get_slaves(self):
        """Round-robin slave balancer"""
        self._slaves = yield from self._sentinel.discover_slaves(
            self._service_name)
        return self._slaves


class RedisSentinel:
    """Redis Sentinel cluster client"""
    def __init__(self, sentinels, min_other_sentinels=0, loop=None):
        self._sentinels = sentinels
        self._min_other_sentinels = min_other_sentinels
        self._loop = loop or asyncio.get_event_loop()
        self._services = {}

    def num_sentinels(self):
        return len(self._sentinels)

    def get_sentinel_connection(self, idx):
        return self._sentinels[idx]

    def close(self):
        for sentinel in self._sentinels:
            sentinel.close()

    @asyncio.coroutine
    def wait_closed(self):
        for sentinel in self._sentinels:
            yield from sentinel.wait_closed()

    def check_master_state(self, state, service_name):
        if not state['is_master'] or state['is_sdown'] or state['is_odown']:
            return False
        # Check if our sentinel doesn't see other nodes
        if state['num-other-sentinels'] < self._min_other_sentinels:
            return False
        return True

    @asyncio.coroutine
    def discover_master(self, service_name):
        """Asks sentinel servers for the Redis master's address corresponding
        to the service labeled ``service_name``.
        Returns a pair (address, port) or raises MasterNotFoundError if no
        master is found.
        """
        for sentinel_no, sentinel in enumerate(self._sentinels):
            try:
                masters = yield from sentinel.sentinel_masters()
            except RedisError:
                continue
            state = masters.get(service_name)
            if state and self.check_master_state(state, service_name):
                # Put this sentinel at the top of the list
                self._sentinels[0], self._sentinels[sentinel_no] = (
                    sentinel, self._sentinels[0])
                return state['ip'], state['port']
        raise MasterNotFoundError("No master found for %r" % (service_name,))

    def filter_slaves(self, slaves):
        """Remove slaves that are in an ODOWN or SDOWN state"""
        slaves_alive = []
        for slave in slaves:
            if slave['is_odown'] or slave['is_sdown']:
                continue
            slaves_alive.append((slave['ip'], slave['port']))
        return slaves_alive

    @asyncio.coroutine
    def discover_slaves(self, service_name):
        """Returns a list of alive slaves for service ``service_name``"""
        for sentinel in self._sentinels:
            try:
                slaves = yield from sentinel.sentinel_slaves(service_name)
            except RedisError:
                continue
            slaves = self.filter_slaves(slaves)
            if slaves:
                return slaves
        return []

    @asyncio.coroutine
    def master_for(self, service_name, **kwargs):
        """Returns a redis client instance for the ``service_name`` master.
        A SentinelConnectionPool class is used to retrive the master's
        address before establishing a new connection.

        NOTE: If the master's address has changed, any cached connections to
        the old master are closed.

        All other keyword arguments are merged with any connection_kwargs
        passed to this class and passed to the connection pool as keyword
        arguments to be used to initialize Redis connections.
        """
        connection_kwargs = dict()
        connection_kwargs.update(kwargs)
        if service_name not in self._services:
            self._services[service_name] = {}
        if 'master' not in self._services[service_name]:
            self._services[service_name]['master'] = \
                RedisSentinelService(service_name, self, True)
        service = self._services[service_name]['master']
        conn = yield from create_sentinel_connection(service,
                                                     **connection_kwargs)
        return conn

    @asyncio.coroutine
    def slave_for(self, service_name, **kwargs):
        """Returns redis client instance for the ``service_name`` slave(s).

        A SentinelConnectionPool class is used to retrive the slave's
        address before establishing a new connection.

        All other keyword arguments are merged with any connection_kwargs
        passed to this class and passed to the connection pool as keyword
        arguments to be used to initialize Redis connections.
        """
        connection_kwargs = dict()
        connection_kwargs.update(kwargs)
        if service_name not in self._services:
            self._services[service_name] = {}
        if 'slave' not in self._services[service_name]:
            self._services[service_name]['slave'] = \
                RedisSentinelService(service_name, self, False)
        service = self._services[service_name]['slave']
        conn = yield from create_sentinel_connection(service,
                                                     **connection_kwargs)
        return conn
