import aioredis.extra
from ._testutil import RedisTest, run_until_complete


class LockTest(RedisTest):

    @run_until_complete
    def test_lock_success(self):
        with (yield from aioredis.extra.Lock(self.redis,
                                             "TEST_KEY",
                                             timeout=3)):
            acquired = yield from self.redis.exists("TEST_KEY")
            self.assertEqual(acquired, True)
        released = yield from self.redis.exists("TEST_KEY")
        self.assertEqual(released, True)

    @run_until_complete
    def test_lock_acquired(self):
        yield from self.redis.set("TEST_KEY", "test")
        with self.assertRaises(aioredis.extra.LockError):
            with (yield from aioredis.extra.Lock(self.redis, "TEST_KEY", 3)):
                pass
        yield from self.flushall()
