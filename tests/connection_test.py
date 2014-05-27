import asyncio

from aioredis._testutil import BaseTest
from aioredis import create_connection


class ConnectionTest(BaseTest):

    def test_connect(self):
        @asyncio.coroutine
        def connect():
            conn = yield from create_connection(
                ('localhost', self.redis_port),
                db=0, loop=self.loop)
            self.assertEqual(conn.db, 0)
            return conn
        return self.loop.run_until_complete(connect())
