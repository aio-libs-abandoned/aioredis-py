import asyncio
import unittest

from ._testutil import BaseTest, run_until_complete
from aioredis import create_redis


@unittest.skip('hyperloglog works only with redis>=2.8.9')
class StringCommandsTest(BaseTest):

    def setUp(self):
        super().setUp()
        self.redis = self.loop.run_until_complete(create_redis(
            ('localhost', self.redis_port), loop=self.loop))

    def tearDown(self):
        self.redis.close()
        del self.redis
        super().tearDown()

    @asyncio.coroutine
    def add(self, key, value):
        ok = yield from self.redis.connection.execute('hset', key, field, value)
        self.assertEqual(ok, b'OK')