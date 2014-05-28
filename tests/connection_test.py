import asyncio

from ._testutil import BaseTest
from aioredis import create_connection, ReplyError


class ConnectionTest(BaseTest):

    def test_connects(self):
        for address in [('localhost', self.redis_port),
                        self.redis_socket]:
            with self.subTest("address: {!r}".format(address)):
                conn = self.loop.run_until_complete(create_connection(
                    address, db=0, loop=self.loop))
                self.assertEqual(conn.db, 0)

    def test_global_loop(self):
        asyncio.set_event_loop(self.loop)

        conn = self.loop.run_until_complete(create_connection(
            ('localhost', self.redis_port), db=0))
        self.assertEqual(conn.db, 0)
        self.assertIs(conn._loop, self.loop)

    def test_select_db(self):
        address = ('localhost', self.redis_port)
        conn = self.loop.run_until_complete(create_connection(
            address, db=None, loop=self.loop))

        self.assertIsNone(conn.db)

        for db, exc in [(-1, ValueError),
                        (1.0, TypeError),
                        ('bad value', TypeError),
                        (10000, ReplyError),
                        ]:
            with self.subTest("tryig db: {}, expecting: {}".format(db, exc)):
                with self.assertRaises(exc):
                    self.loop.run_until_complete(create_connection(
                        address, db=db, loop=self.loop))
