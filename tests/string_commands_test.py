import asyncio

from ._testutil import BaseTest, run_until_complete
from aioredis import create_redis, ReplyError


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
        ok = yield from self.redis.connection.execute('set', key, value)
        self.assertEqual(ok, b'OK')

    @run_until_complete
    def test_append(self):
        len_ = yield from self.redis.append('my-key', 'Hello')
        self.assertEqual(len_, 5)
        len_ = yield from self.redis.append('my-key', ', world!')
        self.assertEqual(len_, 13)

        val = yield from self.redis.connection.execute('GET', 'my-key')
        self.assertEqual(val, b'Hello, world!')

        with self.assertRaises(TypeError):
            yield from self.redis.append(None, 'value')
        with self.assertRaises(ReplyError):
            yield from self.redis.append('none-key', None)

    @run_until_complete
    def test_bitcount(self):
        yield from self.add('my-key', b'\x00\x10\x01')

        ret = yield from self.redis.bitcount('my-key')
        self.assertEqual(ret, 2)
        ret = yield from self.redis.bitcount('my-key', 0, 0)
        self.assertEqual(ret, 0)
        ret = yield from self.redis.bitcount('my-key', 1, 1)
        self.assertEqual(ret, 1)
        ret = yield from self.redis.bitcount('my-key', 2, 2)
        self.assertEqual(ret, 1)
        ret = yield from self.redis.bitcount('my-key', 0, 1)
        self.assertEqual(ret, 1)
        ret = yield from self.redis.bitcount('my-key', 0, 2)
        self.assertEqual(ret, 2)
        ret = yield from self.redis.bitcount('my-key', 1, 2)
        self.assertEqual(ret, 2)
        ret = yield from self.redis.bitcount('my-key', 2, 3)
        self.assertEqual(ret, 1)
        ret = yield from self.redis.bitcount('my-key', 0, -1)
        self.assertEqual(ret, 2)

    @run_until_complete
    def test_get(self):
        yield from self.add('my-key', 'value')
        ret = yield from self.redis.get('my-key')
        self.assertEqual(ret, b'value')

        yield from self.add('my-key', 123)
        ret = yield from self.redis.get('my-key')
        self.assertEqual(ret, b'123')

        ret = yield from self.redis.get('bad-key')
        self.assertIsNone(ret)

        with self.assertRaises(TypeError):
            yield from self.redis.get(None)

    @run_until_complete
    def test_set(self):
        ok = yield from self.redis.set('my-key', 'value')
        self.assertEqual(ok, b'OK')

        with self.assertRaises(TypeError):
            yield from self.redis.set(None, 'value')
