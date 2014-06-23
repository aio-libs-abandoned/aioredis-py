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
        key, field, value = b'key:hdel', b'bar', b'zap'
        yield from self.add(key, field, value)

        result = yield from self.redis.hdel(key, field)
        self.assertEqual(result, 1)
        result = yield from self.redis.hdel(key, field)
        self.assertEqual(result, 0)

    @run_until_complete
    def test_hexists(self):
        key, field, value = b'key:hexists', b'bar', b'zap'
        yield from self.add(key, field, value)

        result = yield from self.redis.hexists(key, field)
        self.assertEqual(result, 1)
        result = yield from self.redis.hexists(key, b'not:' + field)
        self.assertEqual(result, 0)
        result = yield from self.redis.hexists(b'not:' + key, field)
        self.assertEqual(result, 0)

    @run_until_complete
    def test_hget(self):

        key, field, value = b'key:hget', b'bar', b'zap'
        yield from self.add(key, field, value)

        test_value = yield from self.redis.hget(key, field)
        self.assertEqual(test_value, value)

        test_value = yield from self.redis.hget(key, b'baz')
        self.assertEqual(test_value, None)

        test_value = yield from self.redis.hget(b'not:key:hget', b'baz')
        self.assertEqual(test_value, None)

    @run_until_complete
    def test_hgetall(self):
        key = b'key:hgetall'
        field1, field2 = b'foo', b'bar'
        value1, value2 = b'baz', b'zap'
        yield from self.add(key, field1, value1)
        yield from self.add(key, field2, value2)

        test_value = yield from self.redis.hgetall(key)

        ref_set = {field1, field2, value1, value2}
        self.assertEqual(ref_set, set(test_value))

        test_value = yield from self.redis.hgetall(b'not:' + key)
        self.assertEqual(test_value, [])

    @run_until_complete
    def test_hincrby(self):
        key, field, value = b'key:hincrby', b'bar', 1
        yield from self.add(key, field, value)

        result = yield from self.redis.hincrby(key, field, 2)
        self.assertEqual(result, 3)

        result = yield from self.redis.hincrby(b'not:' + key, field, 2)
        self.assertEqual(result, 2)
        result = yield from self.redis.hincrby(key, b'not:' + field, 2)
        self.assertEqual(result, 2)

        with self.assertRaises(ReplyError):
            yield from self.redis.hincrby(key, b'not:' + field, 3.14)

    @run_until_complete
    def test_hincrbyfloat(self):
        key, field, value = b'key:hincrbyfloat', b'bar', 2.71
        yield from self.add(key, field, value)

        result = yield from self.redis.hincrbyfloat(key, field, 3.14)
        self.assertEqual(result, b'5.85')
        result = yield from self.redis.hincrbyfloat(b'not:' + key, field, 3.14)
        self.assertEqual(result, b'3.14')
        result = yield from self.redis.hincrbyfloat(key, b'not:' + field, 3.14)
        self.assertEqual(result, b'3.14')

    @run_until_complete
    def test_hkeys(self):
        key = b'key:hkeys'
        field1, field2 = b'foo', b'bar'
        value1, value2 = b'baz', b'zap'
        yield from self.add(key, field1, value1)
        yield from self.add(key, field2, value2)

        test_value = yield from self.redis.hkeys(key)
        self.assertEqual(set(test_value), {field1, field2})

        test_value = yield from self.redis.hkeys(b'not:' + key)
        self.assertEqual(test_value, [])

    @run_until_complete
    def test_hlen(self):
        key = b'key:hlen'
        field1, field2 = b'foo', b'bar'
        value1, value2 = b'baz', b'zap'
        yield from self.add(key, field1, value1)
        yield from self.add(key, field2, value2)

        test_value = yield from self.redis.hlen(key)
        self.assertEqual(test_value, 2)

        test_value = yield from self.redis.hlen(b'not:' + key)
        self.assertEqual(test_value, 0)

    @run_until_complete
    def test_hmget(self):
        key = b'key:hmget'
        field1, field2 = b'foo', b'bar'
        value1, value2 = b'baz', b'zap'
        yield from self.add(key, field1, value1)
        yield from self.add(key, field2, value2)

        test_value = yield from self.redis.hmget(key, field1, field2)
        self.assertEqual(set(test_value), {value1, value2})

        test_value = yield from self.redis.hmget(
            key, b'not:' + field1, b'not:' + field2)
        self.assertEqual([None, None], test_value)

    @run_until_complete
    def test_hmset(self):
        key, field, value = b'key:hmset', b'bar', b'zap'
        yield from self.add(key, field, value)

        # key and field exists
        test_value = yield from self.redis.hmset(key, field, b'baz')
        self.assertEqual(test_value, b'OK')

        result = yield from self.redis.hexists(key, field)
        self.assertEqual(result, 1)

        # key and field does not exists
        test_value = yield from self.redis.hmset(b'not:' + key, field, value)
        self.assertEqual(test_value, b'OK')
        result = yield from self.redis.hexists(b'not:' + key, field)
        self.assertEqual(result, 1)

        # set multiple
        pairs = [b'foo', b'baz', b'bar', b'paz']
        test_value = yield from self.redis.hmset(key, *pairs)
        self.assertEqual(test_value, b'OK')
        test_value = yield from self.redis.hmget(key, b'foo', b'bar')
        self.assertEqual(set(test_value), {b'baz', b'paz'})

    @run_until_complete
    def test_hset(self):
        key, field, value = b'key:hset', b'bar', b'zap'
        test_value = yield from self.redis.hset(key, field, value)
        self.assertEqual(test_value, 1)

        test_value = yield from self.redis.hset(key, field, value)
        self.assertEqual(test_value, 0)

        test_value = yield from self.redis.hset(b'not:' + key, field, value)
        self.assertEqual(test_value, 1)

        result = yield from self.redis.hexists(b'not:' + key, field)
        self.assertEqual(result, 1)

    @run_until_complete
    def test_hsetnx(self):
        key, field, value = b'key:hsetnx', b'bar', b'zap'
        test_value = yield from self.redis.hsetnx(key, field, value)
        self.assertEqual(test_value, 1)
        result = yield from self.redis.hget(key, field)
        self.assertEqual(result, value)

        test_value = yield from self.redis.hsetnx(key, field, b'baz')
        self.assertEqual(test_value, 0)
        result = yield from self.redis.hget(key, field)
        self.assertEqual(result, value)

    @run_until_complete
    def test_hvals(self):
        key = b'key:hvals'
        field1, field2 = b'foo', b'bar'
        value1, value2 = b'baz', b'zap'
        yield from self.add(key, field1, value1)
        yield from self.add(key, field2, value2)

        test_value = yield from self.redis.hvals(key)
        self.assertEqual(set(test_value), {value1, value2})

        test_value = yield from self.redis.hvals(b'not:' + key)
        self.assertEqual(test_value, [])

    @run_until_complete
    def test_hscan(self):
        key = b'key:hscan'
        for i in range(1, 11):
            foo_or_bar = 'bar' if i % 3 else 'foo'
            f = 'field:{}:{}'.format(foo_or_bar, i).encode('utf-8')
            v = 'value:{}'.format(i).encode('utf-8')
            yield from self.add(key, f, v)

        cursor, values = yield from self.redis.hscan(key, match=b'field:foo:*')
        self.assertEqual(len(values), 3*2)
        cursor, values = yield from self.redis.hscan(key, match=b'field:bar:*')
        self.assertEqual(len(values), 7*2)

        # SCAN family functions do not guarantee that the number of
        # elements returned per call are in a given range. So here
        # just dummy test, that *count* argument does not break something
        cursor = 0
        test_values = []
        for i in range(11):
            cursor, values = yield from self.redis.hscan(key, cursor, count=1)
            test_values.extend(values)
            if not int(cursor):
                break
        self.assertEqual(len(test_values), 10*2)
