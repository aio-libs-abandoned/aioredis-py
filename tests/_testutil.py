import asyncio
import os
import re
import unittest

from functools import wraps
from aioredis import create_redis, create_connection, create_pool, create_cluster


REDIS_VERSION = os.environ.get('REDIS_VERSION')
if not REDIS_VERSION:
    REDIS_VERSION = (0, 0, 0)
else:
    res = re.findall('(\d\.\d\.\d+)', REDIS_VERSION)
    if res:
        REDIS_VERSION = tuple(map(int, res[0].split('.')))
    else:
        REDIS_VERSION = (0, 0, 0)

IS_REDIS_CLUSTER = os.environ.get('REDIS_CLUSTER') == 'true'
SLOT_ZERO_KEY = 'key:24358' # is mapped to keyslot 0

def run_until_complete(fun):
    if not asyncio.iscoroutinefunction(fun):
        fun = asyncio.coroutine(fun)

    @wraps(fun)
    def wrapper(test, *args, **kw):
        loop = test.loop
        ret = loop.run_until_complete(
            asyncio.wait_for(fun(test, *args, **kw), 15, loop=loop))
        return ret
    return wrapper


class BaseTest(unittest.TestCase):
    """Base test case for unittests.
    """

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.redis_port = int(os.environ.get('REDIS_PORT') or 6379)
        socket = os.environ.get('REDIS_SOCKET')
        self.redis_socket = socket or '/tmp/aioredis.sock'
        self._conns = []
        self._redises = []
        self._pools = []
        self._clusters = []

    def tearDown(self):
        waiters = []
        while self._conns:
            conn = self._conns.pop(0)
            conn.close()
            waiters.append(conn.wait_closed())
        while self._redises:
            redis = self._redises.pop(0)
            redis.close()
            waiters.append(redis.wait_closed())
        while self._pools:
            pool = self._pools.pop(0)
            waiters.append(pool.clear())
        while self._clusters:
            cluster = self._clusters.pop(0)
            waiters.append(cluster.clear())

        if waiters:
            self.loop.run_until_complete(
                asyncio.gather(*waiters, loop=self.loop))
        self.loop.close()
        del self.loop

    @asyncio.coroutine
    def create_connection(self, *args, **kw):
        conn = yield from create_connection(*args, **kw)
        self._conns.append(conn)
        return conn

    @asyncio.coroutine
    def create_redis(self, *args, **kw):
        redis = yield from create_redis(*args, **kw)
        self._redises.append(redis)
        return redis

    @asyncio.coroutine
    def create_pool(self, *args, **kw):
        pool = yield from create_pool(*args, **kw)
        self._pools.append(pool)
        return pool

    @asyncio.coroutine
    def create_cluster(self, *args, **kw):
        cluster = yield from create_cluster(*args, **kw)
        self._clusters.append(cluster)
        return cluster

    @staticmethod
    def get_cluster_nodes(start_port):
        ports = [start_port + i for i in range(6)]
        nodes = [('localhost', port) for port in ports]
        return nodes

    @asyncio.coroutine
    def create_test_redis_or_cluster(self, encoding=None):
        if not IS_REDIS_CLUSTER:
            return self.create_redis(('localhost', self.redis_port), encoding=encoding, loop=self.loop)
        else:
            nodes = self.get_cluster_nodes(self.redis_port)
            return self.create_cluster(nodes, loop=self.loop, encoding=encoding)

    @asyncio.coroutine
    def create_test_connection_for_key(self, key, encoding=None):
        if IS_REDIS_CLUSTER:
            node = self.redis.get_node()


class RedisTest(BaseTest):

    def setUp(self):
        super().setUp()
        self.redis = self.loop.run_until_complete(self.create_test_redis_or_cluster())

    def tearDown(self):
        del self.redis
        super().tearDown()

    @asyncio.coroutine
    def execute(self, command, *args):
        if not IS_REDIS_CLUSTER:
            return (yield from self.redis.connection.execute(command, *args))
        else:
            address = self.redis.get_node(*args).address
            redis = yield from self.redis.create_connection(address)
            try:
                return (yield from redis.connection.execute(command, *args))
            finally:
                redis.close()
                yield from redis.wait_closed()

    @asyncio.coroutine
    def add(self, key, value):
        ok = yield from self.execute('set', key, value)
        self.assertEqual(ok, b'OK')

    @asyncio.coroutine
    def flushall(self):
        if not IS_REDIS_CLUSTER:
            ok = yield from self.redis.connection.execute('flushall')
            self.assertEqual(ok, b'OK')
        else:
            ok = yield from self.redis.execute('flushall')
            self.assertTrue(ok)


class RedisEncodingTest(BaseTest):

    def setUp(self):
        super().setUp()
        self.redis = self.loop.run_until_complete(self.create_test_redis_or_cluster(encoding='utf-8'))

    def tearDown(self):
        del self.redis
        super().tearDown()
