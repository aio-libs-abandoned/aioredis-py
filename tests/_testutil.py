import asyncio
import functools
import os
import re
import unittest
import unittest.mock

from functools import wraps
from unittest.case import SkipTest
from setupcluster import START_PORT as CLUSTER_START_PORT

from aioredis import (
    create_redis, create_connection, create_pool, create_cluster,
    create_pool_cluster, Redis
)


REDIS_VERSION = os.environ.get('REDIS_VERSION')
if not REDIS_VERSION:
    REDIS_VERSION = (0, 0, 0)
else:
    res = re.findall('(\d\.\d\.\d+)', REDIS_VERSION)
    if res:
        REDIS_VERSION = tuple(map(int, res[0].split('.')))
    else:
        REDIS_VERSION = (0, 0, 0)

SLOT_ZERO_KEY = 'key:24358'  # is mapped to keyslot 0


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
    _running_on_cluster = False

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        if not self.running_on_cluster:
            self.redis_port = int(os.environ.get('REDIS_PORT') or 6379)
        else:
            self.redis_port = int(
                os.environ.get('REDIS_CLUSTER_PORT') or CLUSTER_START_PORT)
        socket = os.environ.get('REDIS_SOCKET')
        self.redis_socket = socket or '/tmp/aioredis.sock'
        self._conns = []
        self._redises = []
        self._pools = []
        self._clusters = []
        self._pool_clusters = []

    @property
    def running_on_cluster(self):
        # The attribute is set by the runtests script. The environment
        # variable can be used to configure test runners in IDEs.
        return (self._running_on_cluster or
                os.environ.get('REDIS_CLUSTER') == 'true')

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
        while self._pool_clusters:
            cluster = self._pool_clusters.pop(0)
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

    @asyncio.coroutine
    def create_pool_cluster(self, *args, **kw):
        cluster = yield from create_pool_cluster(*args, **kw)
        self._pool_clusters.append(cluster)
        return cluster

    @staticmethod
    def get_cluster_addresses(start_port):
        ports = [start_port + i for i in range(6)]
        return [('127.0.0.1', port) for port in ports]

    @asyncio.coroutine
    def create_test_redis_or_cluster(self, encoding=None):
        if not self.running_on_cluster:
            return self.create_redis(
                ('127.0.0.1', self.redis_port),
                encoding=encoding,
                loop=self.loop
            )
        else:
            return self.create_test_cluster(encoding=encoding)

    @asyncio.coroutine
    def create_test_cluster(self, encoding=None):
        nodes = self.get_cluster_addresses(self.redis_port)
        return self.create_cluster(nodes, loop=self.loop, encoding=encoding)


class RedisTest(BaseTest):

    def setUp(self):
        super().setUp()
        self.redis = self.loop.run_until_complete(
            self.create_test_redis_or_cluster())

    def tearDown(self):
        del self.redis
        super().tearDown()

    @asyncio.coroutine
    def execute(self, command, *args):
        if not self.running_on_cluster:
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
        if not self.running_on_cluster:
            ok = yield from self.redis.connection.execute('flushall')
            self.assertEqual(ok, b'OK')
        else:
            ok = yield from self.redis.execute('flushall')
            self.assertTrue(ok)


class RedisEncodingTest(BaseTest):

    def setUp(self):
        super().setUp()
        self.redis = self.loop.run_until_complete(
            self.create_test_redis_or_cluster(encoding='utf-8'))

    def tearDown(self):
        del self.redis
        super().tearDown()


class FakeConnection:
    def __init__(self, test_case, port, return_value=b'OK'):
        self.port = port
        self.was_used = False

        future = asyncio.Future(loop=test_case.loop)
        if isinstance(return_value, Exception):
            future.set_exception(return_value)
        else:
            future.set_result(return_value)
        self.execute = unittest.mock.Mock(return_value=future)

    def close(self):
        pass

    @asyncio.coroutine
    def wait_closed(self):
        pass


class CreateConnectionMock:
    def __init__(self, test_case, connections):
        assert isinstance(connections, dict)
        self.test_case = test_case
        self.connections = connections
        self.contextManager = unittest.mock.patch(
            'aioredis.commands.create_connection',
            side_effect=self.get_fake_connection
        )

    @asyncio.coroutine
    def get_fake_connection(self, address, db, password, ssl, encoding, loop):
        host, port = address
        self.test_case.assertEqual(host, '127.0.0.1')
        expected_connection = self.connections[port]
        expected_connection.was_used = True
        self.test_case.assertEqual(db, 0)
        self.test_case.assertIsNone(password)
        self.test_case.assertIsNone(encoding)
        return expected_connection

    def __enter__(self):
        self.contextManager.__enter__()
        return self

    def __exit__(self, *args):
        self.contextManager.__exit__(*args)
        self.test_case.assertTrue(
            all(connection.was_used
                for connection in self.connections.values()))


class PoolConnectionMock:
    def __init__(self, test_case, cluster, connections):
        self.test_case = test_case
        self.cluster = cluster
        self.connections = connections
        self.contextManagers = []

        def create_connection_future(port):
            connection = self.connections[port]
            connection.was_used = True
            future = asyncio.Future(loop=test_case.loop)
            future.set_result(Redis(connection))
            return future

        def create_error_future(port):
            future = asyncio.Future(loop=test_case.loop)
            future.set_exception(AssertionError(
                'No connection expected for port {}.'.format(port)))
            return future

        for pool in cluster.get_nodes_entities():
            port = pool._address[1]
            if port in self.connections:
                create_future = functools.partial(
                    create_connection_future, port)
            else:
                create_future = functools.partial(create_error_future, port)

            self.contextManagers.append(unittest.mock.patch.object(
                pool, 'acquire', side_effect=create_future))
            self.contextManagers.append(unittest.mock.patch.object(
                pool, 'release'))

    def __enter__(self):
        for manager in self.contextManagers:
            manager.__enter__()

    def __exit__(self, *args):
        for manager in reversed(self.contextManagers):
            manager.__exit__(*args)
        self.test_case.assertTrue(
            all(connection.was_used
                for connection in self.connections.values()))


def _create_cluster_test_decorator(is_cluster_test, skip_reason):
    def decorator(function):
        @functools.wraps(function)
        def wrapper(test_case):
            if test_case.running_on_cluster != is_cluster_test:
                raise SkipTest(skip_reason)
            else:
                function(test_case)  # execute test

        return wrapper
    return decorator


def cluster_test(skip_reason='Need a running cluster'):
    return _create_cluster_test_decorator(True, skip_reason)


def no_cluster_test(skip_reason='Not yet implemented'):
    return _create_cluster_test_decorator(False, skip_reason)
