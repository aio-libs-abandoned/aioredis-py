import asyncio

from ._testutil import BaseTest, run_until_complete
from aioredis import create_pool, RedisPool


class PoolTest(BaseTest):

    def _assert_defaults(self, pool):
        self.assertIsInstance(pool, RedisPool)
        self.assertEqual(pool.minsize, 10)
        self.assertEqual(pool.maxsize, 10)
        self.assertEqual(pool.size, 10)
        self.assertEqual(pool.freesize, 10)

    def test_connect(self):
        pool = self.loop.run_until_complete(create_pool(
            ('localhost', self.redis_port), loop=self.loop))
        self._assert_defaults(pool)

    def test_global_loop(self):
        asyncio.set_event_loop(self.loop)

        pool = self.loop.run_until_complete(create_pool(
            ('localhost', self.redis_port)))
        self._assert_defaults(pool)

    @run_until_complete
    def test_clear(self):
        pool = yield from create_pool(
            ('localhost', self.redis_port), loop=self.loop)
        self._assert_defaults(pool)

        yield from pool.clear()
        self.assertEqual(pool.freesize, 0)

    @run_until_complete
    def test_no_yield_from(self):
        pool = yield from create_pool(
            ('localhost', self.redis_port), loop=self.loop)

        with self.assertRaises(RuntimeError):
            with pool:
                pass

    @run_until_complete
    def test_simple_command(self):
        pool = yield from create_pool(
            ('localhost', self.redis_port), loop=self.loop)

        with (yield from pool) as conn:
            msg = yield from conn.echo('hello')
            self.assertEqual(msg, b'hello')

    @run_until_complete
    def xtest_create_new(self):
        pool = yield from create_pool(
            ('localhost', self.redis_port),
            minsize=1, loop=self.loop)
        self.assertEqual(pool.size, 1)
        self.assertEqual(pool.freesize, 1)

        with (yield from pool):
            self.assertEqual(pool.size, 1)
            self.assertEqual(pool.freesize, 0)

            with (yield from pool):
                self.assertEqual(pool.size, 2)
                self.assertEqual(pool.freesize, 0)

        self.assertEqual(pool.size, 2)
        self.assertEqual(pool.freesize, 2)
