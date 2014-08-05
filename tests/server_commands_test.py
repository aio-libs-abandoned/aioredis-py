import time

from ._testutil import RedisTest, run_until_complete


class ServerCommandsTest(RedisTest):

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
    def test_time(self):
        res = yield from self.redis.time()
        self.assertIsInstance(res, float)
        self.assertEqual(int(res), int(time.time()))
