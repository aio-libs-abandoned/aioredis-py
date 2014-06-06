import asyncio
from unittest import mock

from ._testutil import BaseTest, run_until_complete
from aioredis import create_redis, ReplyError


class GenericCommandsTest(BaseTest):

    def setUp(self):
        super().setUp()
        self.redis = self.loop.run_until_complete(create_redis(
            ('localhost', self.redis_port), loop=self.loop))

    def tearDown(self):
        super().tearDown()
        self.redis.close()
        del self.redis

    @asyncio.coroutine
    def add(self, key, value):
        ok = yield from self.redis.connection.execute('set', key, value)
        self.assertEqual(ok, b'OK')

    @run_until_complete
    def test_delete(self):
        yield from self.add('my-key', 123)
        yield from self.add('other-key', 123)

        res = yield from self.redis.delete('my-key', 'non-existent-key')
        self.assertEqual(res, 1)

        res = yield from self.redis.delete('other-key', 'other-key')
        self.assertEqual(res, 1)

        with self.assertRaises(TypeError):
            yield from self.redis.delete(None)

        with self.assertRaises(TypeError):
            yield from self.redis.delete('my-key', 'my-key', None)

    @run_until_complete
    def test_dump(self):
        yield from self.add('my-key', 123)

        data = yield from self.redis.dump('my-key')
        self.assertEqual(data, mock.ANY)
        self.assertIsInstance(data, (bytes, bytearray))
        self.assertGreater(len(data), 0)

        data = yield from self.redis.dump('non-existent-key')
        self.assertIsNone(data)

        with self.assertRaises(TypeError):
            yield from self.redis.dump(None)

    @run_until_complete
    def test_exists(self):
        yield from self.add('my-key', 123)

        res = yield from self.redis.exists('my-key')
        self.assertIs(res, True)

        res = yield from self.redis.exists('non-existent-key')
        self.assertIs(res, False)

        with self.assertRaises(TypeError):
            yield from self.redis.exists(None)

    @run_until_complete
    def test_expire(self):
        yield from self.add('my-key', 132)

        res = yield from self.redis.expire('my-key', 10)
        self.assertIs(res, True)

        res = yield from self.redis.connection.execute('TTL', 'my-key')
        self.assertGreaterEqual(res, 10)

        yield from self.redis.expire('my-key', -1)
        res = yield from self.redis.exists('my-key')
        self.assertIs(res, False)

        res = yield from self.redis.expire('other-key', 1000)
        self.assertIs(res, False)

        yield from self.add('my-key', 1)
        res = yield from self.redis.expire('my-key', 10.0)
        self.assertIs(res, True)
        res = yield from self.redis.connection.execute('TTL', 'my-key')
        self.assertGreaterEqual(res, 10)

        with self.assertRaises(TypeError):
            yield from self.redis.expire(None, 123)
        with self.assertRaises(TypeError):
            yield from self.redis.expire('my-key', 'timeout')
