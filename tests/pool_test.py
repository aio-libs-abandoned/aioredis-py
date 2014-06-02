import asyncio

from ._testutil import BaseTest
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

    def test_no_yield_from(self):
        pool = self.loop.run_until_complete(create_pool(
            ('localhost', self.redis_port), loop=self.loop))

        with self.assertRaises(RuntimeError):
            with pool:
                pass

    def test_simple_command(self):
        pool = self.loop.run_until_complete(create_pool(
            ('localhost', self.redis_port), loop=self.loop))

        with (yield from pool) as conn:
            msg = yield from conn.echo('hello')
            self.assertEqual(msg, b'hello')
