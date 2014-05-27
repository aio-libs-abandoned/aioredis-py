import asyncio

from aioredis._testutil import BaseTest
from aioredis import RedisConnection


class ConnectionTest(BaseTest):

    def test_connect(self):
        @asyncio.coroutine
        def connect():
            conn = RedisConnection(loop=self.loop)
            yield from conn.connect(('localhost', self.redis_port), db=0)
            self.assertEqual(conn.db, 0)
            return conn
        return self.loop.run_until_complete(connect())
