import os
import asyncio

from aioredis import Redis, create_redis
from aioredis.extra.lock import Lock, LockMixin
from ._testutil import RedisTest, run_until_complete


class RedisWithLock(Redis, LockMixin): pass


class LockTest(RedisTest):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        self.redis_port = int(os.environ.get('REDIS_PORT') or 6379)
        socket = os.environ.get('REDIS_SOCKET')
        self.redis_socket = socket or '/tmp/aioredis.sock'
        self.redis = self.loop.run_until_complete(create_redis(
            ('localhost', self.redis_port), commands_factory=RedisWithLock,
            loop=self.loop))

    @run_until_complete
    def test_lock_success(self):
        yield from self.redis.register_lock_scripts()
        lock = Lock(self.redis, "TEST_KEY", timeout=3)
        acquired = yield from lock.acquire()
        self.assertEqual(acquired, True)
        released = yield from lock.release()
        self.assertEqual(released, True)

    @run_until_complete
    def test_lock_acquired(self):
        yield from self.redis.register_lock_scripts()
        yield from self.redis.set("TEST_KEY", "test")
        lock = Lock(self.redis, "TEST_KEY", blocking=False)
        acquired = yield from lock.acquire()
        self.assertEqual(acquired, False)
        yield from self.flushall()

    @run_until_complete
    def test_lock_mixin(self):
        yield from self.redis.register_lock_scripts()
        lock = self.redis.lock("TEST_KEY", timeout=3)
        self.assertTrue(isinstance(lock, Lock))
        self.assertEqual(lock.key, "TEST_KEY")
        self.assertEqual(lock.timeout, 3)

    @run_until_complete
    def test_execute_in_lock(self):
        yield from self.redis.register_lock_scripts()

        @asyncio.coroutine
        def task():
            acquired = yield from self.redis.get("TEST_KEY")
            if not acquired:
                return
            raise ValueError

        with self.assertRaises(ValueError):
            yield from self.redis.execute_in_lock(task(), "TEST_KEY")

        exists = yield from self.redis.exists("TEST_KEY")
        self.assertEqual(exists, False)
