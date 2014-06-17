import asyncio

from ._testutil import BaseTest, run_until_complete
from aioredis import create_redis, ReplyError


class HashCommandsTest(BaseTest):

    def setUp(self):
        super().setUp()
        self.redis = self.loop.run_until_complete(create_redis(
            ('localhost', self.redis_port), loop=self.loop))

    def tearDown(self):
        self.redis.close()
        del self.redis
        super().tearDown()

    @asyncio.coroutine
    def add(self, key, field, value):
        ok = yield from self.redis.connection.execute(
            b'hset', key, field, value)
        self.assertEqual(ok, 1)

    @run_until_complete
    def test_hdel(self):
        key, field, value = 'key:hdel', 'bar', 'zap'
        yield from self.add(key, field, value)

        result = yield from self.redis.hdel(key, field)
        self.assertEqual(result, 1)
        result = yield from self.redis.hdel(key, field)
        self.assertEqual(result, 0)

    @run_until_complete
    def test_hexists(self):
        key, field, value = 'key:hexists', 'bar', 'zap'
        yield from self.add(key, field, value)

        result = yield from self.redis.hexists(key, field)
        self.assertEqual(result, 1)
        result = yield from self.redis.hexists(key, 'not:' + field)
        self.assertEqual(result, 0)
        result = yield from self.redis.hexists('not:' + key, field)
        self.assertEqual(result, 0)

    @run_until_complete
    def test_hget(self):

        key, field, value = 'key:hget', 'bar', 'zap'
        yield from self.add(key, field, value)

        test_value = yield from self.redis.hget(key, field)
        self.assertEqual(test_value, value)

        test_value = yield from self.redis.hget(key, 'baz')
        self.assertEqual(test_value, None)

        test_value = yield from self.redis.hget('not:key:hget', 'baz')
        self.assertEqual(test_value, None)

    @run_until_complete
    def test_hgetall(self):
        key = 'key:hgetall'
        field1, field2 = 'foo', 'bar'
        value1, value2 = 'baz', 'zap'
        yield from self.add(key, field1, value1)
        yield from self.add(key, field2, value2)

        test_value = yield from self.redis.hgetall(key)

        ref_set = {field1, field2, value1, value2}
        self.assertEqual(ref_set, set(test_value))

        test_value = yield from self.redis.hgetall('not:' + key)
        self.assertEqual(test_value, [])

    @run_until_complete
    def test_hincrby(self):
        key, field, value = 'key:hincrby', 'bar', 1
        yield from self.add(key, field, value)

        result = yield from self.redis.hincrby(key, field, 2)
        self.assertEqual(result, 3)

        result = yield from self.redis.hincrby('not:' + key, field, 2)
        self.assertEqual(result, 2)
        result = yield from self.redis.hincrby(key, 'not:' + field, 2)
        self.assertEqual(result, 2)

        with self.assertRaises(ReplyError):
            yield from self.redis.hincrby(key, 'not:' + field, 3.14)

    @run_until_complete
    def test_hincrbyfloat(self):
        key, field, value = 'key:hincrbyfloat', 'bar', 2.71
        yield from self.add(key, field, value)

        result = yield from self.redis.hincrbyfloat(key, field, 3.14)
        self.assertEqual(result, 5.85)
        result = yield from self.redis.hincrbyfloat('not:' + key, field, 3.14)
        self.assertEqual(result, 3.14)
        result = yield from self.redis.hincrbyfloat(key, 'not:' + field, 3.14)
        self.assertEqual(result, 3.14)

    @run_until_complete
    def test_hlen(self):
        key = 'key:hlen'
        field1, field2 = 'foo', 'bar'
        value1, value2 = 'baz', 'zap'
        yield from self.add(key, field1, value1)
        yield from self.add(key, field2, value2)

        test_value = yield from self.redis.hlen(key)
        self.assertEqual(test_value, 2)

        test_value = yield from self.redis.hlen('not:' + key)
        self.assertEqual(test_value, 0)

    @run_until_complete
    def test_hmget(self):
        key = 'key:hmget'
        field1, field2 = 'foo', 'bar'
        value1, value2 = 'baz', 'zap'
        yield from self.add(key, field1, value1)
        yield from self.add(key, field2, value2)

        test_value = yield from self.redis.hmget(key, field1, field2)
        self.assertEqual(set(test_value), {value1, value2})

        test_value = yield from self.redis.hlen('not:' + key)
        self.assertEqual(test_value, None)

        test_value = yield from self.redis.hmget(
            key, 'not:' + field1, 'not:' + field2)
        self.assertEqual([None, None], test_value)

    @run_until_complete
    def test_hset(self):
        key, field, value = 'key:hset', 'bar', 'zap'
        test_value = yield from self.redis.hset(key, field, value)
        self.assertEqual(test_value, 1)

        test_value = yield from self.redis.hset(key, field, value)
        self.assertEqual(test_value, 0)

        result = yield from self.redis.hexists(key, field)
        self.assertEqual(result, 1)

