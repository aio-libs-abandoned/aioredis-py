import asyncio

from ._testutil import BaseTest
from aioredis import create_connection, ReplyError


class ConnectionTest(BaseTest):

    def test_connect_tcp(self):
        conn = self.loop.run_until_complete(create_connection(
            ('localhost', self.redis_port), loop=self.loop))
        self.assertEqual(conn.db, 0)

    def test_connect_unixsocket(self):
        conn = self.loop.run_until_complete(create_connection(
            self.redis_socket, db=0, loop=self.loop))
        self.assertEqual(conn.db, 0)

    def test_global_loop(self):
        asyncio.set_event_loop(self.loop)

        conn = self.loop.run_until_complete(create_connection(
            ('localhost', self.redis_port), db=0))
        self.assertEqual(conn.db, 0)
        self.assertIs(conn._loop, self.loop)

    def xtest_select_db(self):
        address = ('localhost', self.redis_port)
        conn = self.loop.run_until_complete(create_connection(
            address, loop=self.loop))
        self.assertEqual(conn.db, 0)

        with self.assertRaises(ReplyError):
            self.loop.run_until_complete(create_connection(
                address, db=-1, loop=self.loop))
        with self.assertRaises(ReplyError):
            self.loop.run_until_complete(create_connection(
                address, db=1.0, loop=self.loop))
        with self.assertRaises(ReplyError):
            self.loop.run_until_complete(create_connection(
                address, db=100000, loop=self.loop))
        with self.assertRaises(ReplyError):
            self.loop.run_until_complete(create_connection(
                address, db='bad value', loop=self.loop))
