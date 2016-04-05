import unittest

from aioredis import ConnectionClosedError, ReplyError

from ._testutil import RedisTest, run_until_complete, IS_REDIS_CLUSTER


class ConnectionCommandsTest(RedisTest):
    @run_until_complete
    def test_repr(self):
        redis = yield from self.create_redis(
            ('localhost', self.redis_port), db=0, loop=self.loop)
        self.assertEqual(repr(redis), '<Redis <RedisConnection [db:0]>>')

        if not IS_REDIS_CLUSTER:
            redis = yield from self.create_redis(
                ('localhost', self.redis_port), db=1, loop=self.loop)
            self.assertEqual(repr(redis), '<Redis <RedisConnection [db:1]>>')

    @run_until_complete
    def test_auth(self):
        expected_message = "ERR Client sent AUTH, but no password is set"
        with self.assertRaisesRegex(ReplyError, expected_message):
            yield from self.redis.auth('')

    @run_until_complete
    def test_echo(self):
        resp = yield from self.redis.echo('ECHO')
        self.assertEqual(resp, b'ECHO')

        with self.assertRaises(TypeError):
            yield from self.redis.echo(None)

    @run_until_complete
    def test_ping(self):
        resp = yield from self.redis.ping()
        if not IS_REDIS_CLUSTER:
            self.assertEqual(resp, b'PONG')
        else:
            self.assertEqual(resp, [b'PONG'] * 3)

    @unittest.skipIf(IS_REDIS_CLUSTER, 'use cluster.clear instead')
    @run_until_complete
    def test_quit(self):
        resp = yield from self.redis.quit()
        self.assertEqual(resp, b'OK')

        with self.assertRaises(ConnectionClosedError):
            yield from self.redis.ping()

    @unittest.skipIf(IS_REDIS_CLUSTER,
                     'select is not available on Redis cluster')
    @run_until_complete
    def test_select(self):
        self.assertEqual(self.redis.db, 0)

        resp = yield from self.redis.select(1)
        self.assertEqual(resp, True)
        self.assertEqual(self.redis.db, 1)
        self.assertEqual(self.redis.connection.db, 1)

    @run_until_complete
    def test_encoding(self):
        redis = yield from self.create_redis(
            ('localhost', self.redis_port),
            encoding='utf-8',
            loop=self.loop)
        self.assertEqual(redis.encoding, 'utf-8')
