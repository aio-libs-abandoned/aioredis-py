import unittest
import asyncio
import os

from ._testutil import BaseTest, run_until_complete
from aioredis import ReplyError, ProtocolError, RedisError


class ConnectionTest(BaseTest):

    @run_until_complete
    def test_connect_tcp(self):
        conn = yield from self.create_connection(
            ('localhost', self.redis_port), loop=self.loop)
        self.assertEqual(conn.db, 0)
        self.assertEqual(str(conn), '<RedisConnection [db:0]>')

        conn = yield from self.create_connection(
            ['localhost', self.redis_port], loop=self.loop)
        self.assertEqual(conn.db, 0)
        self.assertEqual(str(conn), '<RedisConnection [db:0]>')

    @unittest.skipIf(not os.environ.get('REDIS_SOCKET'), "no redis socket")
    @run_until_complete
    def test_connect_unixsocket(self):
        conn = yield from self.create_connection(
            self.redis_socket, db=0, loop=self.loop)
        self.assertEqual(conn.db, 0)
        self.assertEqual(str(conn), '<RedisConnection [db:0]>')

    def test_global_loop(self):
        asyncio.set_event_loop(self.loop)

        conn = self.loop.run_until_complete(self.create_connection(
            ('localhost', self.redis_port), db=0))
        self.assertEqual(conn.db, 0)
        self.assertIs(conn._loop, self.loop)

    @run_until_complete
    def test_select_db(self):
        address = ('localhost', self.redis_port)
        conn = yield from self.create_connection(address, loop=self.loop)
        self.assertEqual(conn.db, 0)

        with self.assertRaises(ValueError):
            yield from self.create_connection(address, db=-1, loop=self.loop)
        with self.assertRaises(TypeError):
            yield from self.create_connection(address, db=1.0, loop=self.loop)
        with self.assertRaises(TypeError):
            yield from self.create_connection(
                address, db='bad value', loop=self.loop)
        with self.assertRaises(TypeError):
            conn = yield from self.create_connection(
                address, db=None, loop=self.loop)
            yield from conn.select(None)
        with self.assertRaises(ReplyError):
            yield from self.create_connection(
                address, db=100000, loop=self.loop)

        yield from conn.select(1)
        self.assertEqual(conn.db, 1)
        yield from conn.select(2)
        self.assertEqual(conn.db, 2)
        yield from conn.execute('select', 0)
        self.assertEqual(conn.db, 0)
        yield from conn.execute(b'select', 1)
        self.assertEqual(conn.db, 1)

    @run_until_complete
    def test_protocol_error(self):
        loop = self.loop
        conn = yield from self.create_connection(
            ('localhost', self.redis_port), loop=loop)

        reader = conn._reader

        with self.assertRaises(ProtocolError):
            reader.feed_data(b'not good redis protocol response')
            yield from conn.select(1)

        self.assertEqual(len(conn._waiters), 0)

    def test_close_connection__tcp(self):
        loop = self.loop
        conn = loop.run_until_complete(self.create_connection(
            ('localhost', self.redis_port), loop=loop))
        conn.close()
        with self.assertRaises(AssertionError):
            loop.run_until_complete(conn.select(1))

        conn = loop.run_until_complete(self.create_connection(
            ('localhost', self.redis_port), loop=loop))
        with self.assertRaises(AssertionError):
            conn.close()
            fut = conn.select(1)
            loop.run_until_complete(fut)

    @unittest.skipIf(not os.environ.get('REDIS_SOCKET'), "no redis socket")
    @run_until_complete
    def test_close_connection__socket(self):
        conn = yield from self.create_connection(
            self.redis_socket, loop=self.loop)
        conn.close()
        with self.assertRaises(AssertionError):
            yield from conn.select(1)

    @run_until_complete
    def test_auth(self):
        conn = yield from self.create_connection(
            ('localhost', self.redis_port), loop=self.loop)

        res = yield from conn.execute('CONFIG', 'SET', 'requirepass', 'pass')
        self.assertEqual(res, b'OK')

        conn2 = yield from self.create_connection(
            ('localhost', self.redis_port), loop=self.loop)

        with self.assertRaises(ReplyError):
            yield from conn2.select(1)

        res = yield from conn2.auth('pass')
        self.assertEqual(res, True)
        res = yield from conn2.select(1)
        self.assertTrue(res)

        conn3 = yield from self.create_connection(
            ('localhost', self.redis_port), password='pass', loop=self.loop)

        res = yield from conn3.select(1)
        self.assertTrue(res)

        res = yield from conn2.execute('CONFIG', 'SET', 'requirepass', '')
        self.assertEqual(res, b'OK')

    @run_until_complete
    def test_decoding(self):
        conn = yield from self.create_connection(
            ('localhost', self.redis_port), encoding='utf-8', loop=self.loop)
        self.assertEqual(conn.encoding, 'utf-8',)
        res = yield from conn.execute('set', 'key1', 'value')
        self.assertEqual(res, 'OK')
        res = yield from conn.execute('get', 'key1')
        self.assertEqual(res, 'value')

        res = yield from conn.execute('set', 'key1', b'bin-value')
        self.assertEqual(res, 'OK')
        res = yield from conn.execute('get', 'key1')
        self.assertEqual(res, 'bin-value')

        res = yield from conn.execute('get', 'key1', encoding='ascii')
        self.assertEqual(res, 'bin-value')
        res = yield from conn.execute('get', 'key1', encoding=None)
        self.assertEqual(res, b'bin-value')

        with self.assertRaises(UnicodeDecodeError):
            yield from conn.execute('set', 'key1', 'значение')
            yield from conn.execute('get', 'key1', encoding='ascii')

        conn2 = yield from self.create_connection(
            ('localhost', self.redis_port), loop=self.loop)
        res = yield from conn2.execute('get', 'key1', encoding='utf-8')
        self.assertEqual(res, 'значение')

    @run_until_complete
    def test_execute_exceptions(self):
        conn = yield from self.create_connection(
            ('localhost', self.redis_port), loop=self.loop)
        with self.assertRaises(TypeError):
            yield from conn.execute(None)
        with self.assertRaises(TypeError):
            yield from conn.execute("ECHO", None)
        with self.assertRaises(TypeError):
            yield from conn.execute("GET", ('a', 'b'))
        self.assertEqual(len(conn._waiters), 0)

    @run_until_complete
    def test_subscribe_unsubscribe(self):
        conn = yield from self.create_connection(
            ('localhost', self.redis_port), loop=self.loop)

        self.assertEqual(conn.in_pubsub, 0)

        res = yield from conn.execute('subscribe', 'chan:1')
        self.assertEqual(res, [[b'subscribe', b'chan:1', 1]])

        self.assertTrue(conn.in_pubsub, 1)

        res = yield from conn.execute('unsubscribe', 'chan:1')
        self.assertEqual(res, [[b'unsubscribe', b'chan:1', 0]])
        self.assertEqual(conn.in_pubsub, 0)

        res = yield from conn.execute('subscribe', 'chan:1', 'chan:2')
        self.assertEqual(res, [[b'subscribe', b'chan:1', 1],
                               [b'subscribe', b'chan:2', 2],
                               ])
        self.assertEqual(conn.in_pubsub, 2)

        res = yield from conn.execute('unsubscribe', 'non-existent')
        self.assertEqual(res, [[b'unsubscribe', b'non-existent', 2]])
        self.assertEqual(conn.in_pubsub, 2)

        res = yield from conn.execute('unsubscribe', 'chan:1')
        self.assertEqual(res, [[b'unsubscribe', b'chan:1', 1]])
        self.assertEqual(conn.in_pubsub, 1)

    @run_until_complete
    def test_psubscribe_punsubscribe(self):
        conn = yield from self.create_connection(
            ('localhost', 6379), loop=self.loop)
        res = yield from conn.execute('psubscribe', 'chan:*')
        self.assertEqual(res, [[b'psubscribe', b'chan:*', 1]])
        self.assertEqual(conn.in_pubsub, 1)

    @run_until_complete
    def test_bad_command_in_pubsub(self):
        conn = yield from self.create_connection(
            ('localhost', self.redis_port), loop=self.loop)

        res = yield from conn.execute('subscribe', 'chan:1')
        self.assertEqual(res, [[b'subscribe', b'chan:1', 1]])

        msg = "Connection in SUBSCRIBE mode"
        with self.assertRaisesRegex(RedisError, msg):
            yield from conn.execute('select', 1)
        with self.assertRaisesRegex(RedisError, msg):
            conn.execute('get')

    @run_until_complete
    def test_pubsub_messages(self):
        sub = yield from self.create_connection(
            ('localhost', self.redis_port), loop=self.loop)
        pub = yield from self.create_connection(
            ('localhost', self.redis_port), loop=self.loop)
        res = yield from sub.execute('subscribe', 'chan:1')
        self.assertEqual(res, [[b'subscribe', b'chan:1', 1]])

        self.assertIn(b'chan:1', sub.pubsub_channels)
        chan = sub.pubsub_channels[b'chan:1']
        self.assertEqual(chan.name, b'chan:1')
        self.assertTrue(chan.is_active)

        res = yield from pub.execute('publish', 'chan:1', 'Hello!')
        self.assertEqual(res, 1)
        msg = yield from chan.get()
        self.assertEqual(msg, b'Hello!')

        res = yield from sub.execute('psubscribe', 'chan:*')
        self.assertEqual(res, [[b'psubscribe', b'chan:*', 2]])
        self.assertIn(b'chan:*', sub.pubsub_patterns)
        chan2 = sub.pubsub_patterns[b'chan:*']
        self.assertEqual(chan2.name, b'chan:*')
        self.assertTrue(chan2.is_active)

        res = yield from pub.execute('publish', 'chan:1', 'Hello!')
        self.assertEqual(res, 2)

        msg = yield from chan.get()
        self.assertEqual(msg, b'Hello!')
        dest_chan, msg = yield from chan2.get()
        self.assertEqual(dest_chan, b'chan:1')
        self.assertEqual(msg, b'Hello!')

    @run_until_complete
    def test_multiple_subscribe_unsubscribe(self):
        sub = yield from self.create_connection(
            ('localhost', self.redis_port), loop=self.loop)

        res = yield from sub.execute('subscribe', 'chan:1')
        self.assertEqual(res, [[b'subscribe', b'chan:1', 1]])
        res = yield from sub.execute('subscribe', b'chan:1')
        self.assertEqual(res, [[b'subscribe', b'chan:1', 1]])

        res = yield from sub.execute('unsubscribe', 'chan:1')
        self.assertEqual(res, [[b'unsubscribe', b'chan:1', 0]])
        res = yield from sub.execute('unsubscribe', 'chan:1')
        self.assertEqual(res, [[b'unsubscribe', b'chan:1', 0]])

        res = yield from sub.execute('psubscribe', 'chan:*')
        self.assertEqual(res, [[b'psubscribe', b'chan:*', 1]])
        res = yield from sub.execute('psubscribe', 'chan:*')
        self.assertEqual(res, [[b'psubscribe', b'chan:*', 1]])

        res = yield from sub.execute('punsubscribe', 'chan:*')
        self.assertEqual(res, [[b'punsubscribe', b'chan:*', 0]])
        res = yield from sub.execute('punsubscribe', 'chan:*')
        self.assertEqual(res, [[b'punsubscribe', b'chan:*', 0]])
