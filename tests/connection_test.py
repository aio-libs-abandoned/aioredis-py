import asyncio
import unittest

from aioredis._testutil import BaseTest, test_coroutine
from aioredis import RedisConnection


class ConnectionTest(BaseTest):

    def test_connect(self):
        @asyncio.coroutine
        def connect():
            conn = RedisConnection(loop=self.loop)
            yield from conn.connect(('127.0.0.1', self.redis_port), db=0)
            self.assertEqual(conn.db)
            return conn
        # return self.loop.run_until_complete(connect())
