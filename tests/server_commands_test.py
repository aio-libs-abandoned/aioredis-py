import time
import unittest
from unittest import mock

from aioredis import ReplyError
from ._testutil import RedisTest, run_until_complete, REDIS_VERSION, IS_REDIS_CLUSTER


@unittest.skipIf(IS_REDIS_CLUSTER, 'not clear how to implement these on a cluster')
class ServerCommandsTest(RedisTest):
    @run_until_complete
    def test_client_list(self):
        res = yield from self.redis.client_list()
        self.assertIsInstance(res, list)
        res = [dict(i._asdict()) for i in res]
        expected = {
            'addr': mock.ANY,
            'fd': mock.ANY,
            'age': '0',
            'idle': '0',
            'flags': 'N',
            'db': '0',
            'sub': '0',
            'psub': '0',
            'multi': '-1',
            'qbuf': '0',
            'qbuf_free': mock.ANY,
            'obl': '0',
            'oll': '0',
            'omem': '0',
            'events': 'r',
            'cmd': 'client',
            'name': '',
            }
        if REDIS_VERSION >= (2, 8, 12):
            expected['id'] = mock.ANY
        self.assertIn(expected, res)

    @run_until_complete
    @unittest.skipIf(REDIS_VERSION < (2, 9, 50),
                     'CLIENT PAUSE is available since redis>=2.9.50')
    def test_client_pause(self):
        res = yield from self.redis.client_pause(2000)
        self.assertTrue(res)
        ts = time.time()
        yield from self.redis.ping()
        dt = int(time.time() - ts)
        self.assertEqual(dt, 2)

        with self.assertRaises(TypeError):
            yield from self.redis.client_pause(2.0)
        with self.assertRaises(ValueError):
            yield from self.redis.client_pause(-1)

    @run_until_complete
    def test_client_getname(self):
        res = yield from self.redis.client_getname()
        self.assertIsNone(res)
        ok = yield from self.redis.client_setname('TestClient')
        self.assertTrue(ok)

        res = yield from self.redis.client_getname()
        self.assertEqual(res, b'TestClient')
        res = yield from self.redis.client_getname(encoding='utf-8')
        self.assertEqual(res, 'TestClient')

    @run_until_complete
    def test_config_get(self):
        res = yield from self.redis.config_get('port')
        self.assertEqual(res, {'port': str(self.redis_port)})

        res = yield from self.redis.config_get()
        self.assertGreater(len(res), 0)

        res = yield from self.redis.config_get('unknown_parameter')
        self.assertEqual(res, {})

        with self.assertRaises(TypeError):
            yield from self.redis.config_get(b'port')

    @run_until_complete
    def test_config_rewrite(self):
        with self.assertRaises(ReplyError):
            yield from self.redis.config_rewrite()

    @run_until_complete
    def test_config_set(self):
        cur_value = yield from self.redis.config_get('slave-read-only')
        res = yield from self.redis.config_set('slave-read-only', 'no')
        self.assertTrue(res)
        res = yield from self.redis.config_set(
            'slave-read-only', cur_value['slave-read-only'])
        self.assertTrue(res)

        with self.assertRaisesRegex(
                ReplyError, "Unsupported CONFIG parameter"):
            yield from self.redis.config_set('databases', 100)
        with self.assertRaises(TypeError):
            yield from self.redis.config_set(100, 'databases')

    @run_until_complete
    @unittest.skip("Not implemented")
    def test_config_resetstat(self):
        pass

    @run_until_complete
    def test_dbsize(self):
        res = yield from self.redis.dbsize()
        self.assertGreater(res, 0)

        yield from self.redis.flushdb()
        res = yield from self.redis.dbsize()
        self.assertEqual(res, 0)
        yield from self.redis.set('key', 'value')
        res = yield from self.redis.dbsize()
        self.assertEqual(res, 1)

    @run_until_complete
    @unittest.skip("Not implemented")
    def test_info(self):
        pass

    @run_until_complete
    @unittest.skipIf(REDIS_VERSION < (2, 8, 12),
                     'ROLE is available since redis>=2.8.12')
    def test_role(self):
        res = yield from self.redis.role()
        self.assertEqual(dict(res._asdict()), {
            'role': 'master',
            'replication_offset': mock.ANY,
            'slaves': [],
            })

    @run_until_complete
    def test_time(self):
        res = yield from self.redis.time()
        self.assertIsInstance(res, float)
        self.assertEqual(int(res), int(time.time()))
