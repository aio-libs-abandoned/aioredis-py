import asyncio
import sys
import unittest
from textwrap import dedent

from ._testutil import RedisTest, run_until_complete, REDIS_VERSION
from ._testutil import RedisEncodingTest
from aioredis import ReplyError

PY_35 = sys.version_info > (3, 5)


class HashCommandsTest(RedisTest):

    @asyncio.coroutine
    def add(self, key, field, value):
        ok = yield from self.redis.connection.execute(
            b'hset', key, field, value)
        self.assertEqual(ok, 1)

    @run_until_complete
    def test_hdel(self):
        key, field, value = b'key:hdel', b'bar', b'zap'
        yield from self.add(key, field, value)
        # delete value that exists, expected 1
        result = yield from self.redis.hdel(key, field)
        self.assertEqual(result, 1)
        # delete value that does not exists, expected 0
        result = yield from self.redis.hdel(key, field)
        self.assertEqual(result, 0)

        with self.assertRaises(TypeError):
            yield from self.redis.hdel(None, field)

    @run_until_complete
    def test_hexists(self):
        key, field, value = b'key:hexists', b'bar', b'zap'
        yield from self.add(key, field, value)
        # check value that exists, expected 1
        result = yield from self.redis.hexists(key, field)
        self.assertEqual(result, 1)
        # check value when, key exists and field does not, expected 0
        result = yield from self.redis.hexists(key, b'not:' + field)
        self.assertEqual(result, 0)
        # check value when, key not exists, expected 0
        result = yield from self.redis.hexists(b'not:' + key, field)
        self.assertEqual(result, 0)

        with self.assertRaises(TypeError):
            yield from self.redis.hexists(None, field)

    @run_until_complete
    def test_hget(self):

        key, field, value = b'key:hget', b'bar', b'zap'
        yield from self.add(key, field, value)
        # basic test, fetch value and check in to reference
        test_value = yield from self.redis.hget(key, field)
        self.assertEqual(test_value, value)
        # fetch value, when field does not exists
        test_value = yield from self.redis.hget(key, b'not' + field)
        self.assertEqual(test_value, None)
        # fetch value when key does not exists
        test_value = yield from self.redis.hget(b'not:' + key, b'baz')
        self.assertEqual(test_value, None)

        # check encoding
        test_value = yield from self.redis.hget(key, field, encoding='utf-8')
        self.assertEqual(test_value, 'zap')

        with self.assertRaises(TypeError):
            yield from self.redis.hget(None, field)

    @run_until_complete
    def test_hgetall(self):
        yield from self.add('key:hgetall', 'foo', 'baz')
        yield from self.add('key:hgetall', 'bar', 'zap')

        test_value = yield from self.redis.hgetall('key:hgetall')
        self.assertIsInstance(test_value, dict)
        self.assertEqual({b'foo': b'baz', b'bar': b'zap'}, test_value)
        # try to get all values from key that does not exits
        test_value = yield from self.redis.hgetall(b'not:key:hgetall')
        self.assertEqual(test_value, {})

        # check encoding param
        test_value = yield from self.redis.hgetall(
            'key:hgetall', encoding='utf-8')
        self.assertEqual({'foo': 'baz', 'bar': 'zap'}, test_value)

        with self.assertRaises(TypeError):
            yield from self.redis.hgetall(None)

    @run_until_complete
    def test_hincrby(self):
        key, field, value = b'key:hincrby', b'bar', 1
        yield from self.add(key, field, value)
        # increment initial value by 2
        result = yield from self.redis.hincrby(key, field, 2)
        self.assertEqual(result, 3)

        result = yield from self.redis.hincrby(key, field, -1)
        self.assertEqual(result, 2)

        result = yield from self.redis.hincrby(key, field, -100)
        self.assertEqual(result, -98)

        result = yield from self.redis.hincrby(key, field, -2)
        self.assertEqual(result, -100)

        # increment value in case of key or field that does not exists
        result = yield from self.redis.hincrby(b'not:' + key, field, 2)
        self.assertEqual(result, 2)
        result = yield from self.redis.hincrby(key, b'not:' + field, 2)
        self.assertEqual(result, 2)

        with self.assertRaises(ReplyError):
            yield from self.redis.hincrby(key, b'not:' + field, 3.14)

        with self.assertRaises(ReplyError):
            # initial value is float, try to increment 1
            yield from self.add(b'other:' + key, field, 3.14)
            yield from self.redis.hincrby(b'other:' + key, field, 1)

        with self.assertRaises(TypeError):
            yield from self.redis.hincrby(None, field, 2)

    @run_until_complete
    def test_hincrbyfloat(self):
        key, field, value = b'key:hincrbyfloat', b'bar', 2.71
        yield from self.add(key, field, value)

        result = yield from self.redis.hincrbyfloat(key, field, 3.14)
        self.assertEqual(result, 5.85)

        result = yield from self.redis.hincrbyfloat(key, field, -2.71)
        self.assertEqual(result, 3.14)

        result = yield from self.redis.hincrbyfloat(key, field, -100.1)
        self.assertEqual(result, -96.96)

        # increment value in case of key or field that does not exists
        result = yield from self.redis.hincrbyfloat(b'not:' + key, field, 3.14)
        self.assertEqual(result, 3.14)

        result = yield from self.redis.hincrbyfloat(key, b'not:' + field, 3.14)
        self.assertEqual(result, 3.14)

        with self.assertRaises(TypeError):
            yield from self.redis.hincrbyfloat(None, field, 2)

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

        test_value = yield from self.redis.hkeys(key, encoding='utf-8')
        self.assertEqual(set(test_value), {'foo', 'bar'})

        with self.assertRaises(TypeError):
            yield from self.redis.hkeys(None)

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

        with self.assertRaises(TypeError):
            yield from self.redis.hlen(None)

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

        val = yield from self.redis.hincrby(key, 'numeric')
        self.assertEqual(val, 1)
        test_value = yield from self.redis.hmget(
            key, field1, field2, 'numeric', encoding='utf-8')
        self.assertEqual(['baz', 'zap', '1'], test_value)

        with self.assertRaises(TypeError):
            yield from self.redis.hmget(None, field1, field2)

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

        with self.assertRaises(TypeError):
            yield from self.redis.hmset(key, b'foo', b'bar', b'baz')

        with self.assertRaises(TypeError):
            yield from self.redis.hmset(None, *pairs)

    @run_until_complete
    def test_hset(self):
        key, field, value = b'key:hset', b'bar', b'zap'
        test_value = yield from self.redis.hset(key, field, value)
        self.assertEqual(test_value, 1)

        test_value = yield from self.redis.hset(key, field, value)
        self.assertEqual(test_value, 0)

        test_value = yield from self.redis.hset(b'other:' + key, field, value)
        self.assertEqual(test_value, 1)

        result = yield from self.redis.hexists(b'other:' + key, field)
        self.assertEqual(result, 1)

        with self.assertRaises(TypeError):
            yield from self.redis.hset(None, field, value)

    @run_until_complete
    def test_hsetnx(self):
        key, field, value = b'key:hsetnx', b'bar', b'zap'
        # field does not exists, operation should be successful
        test_value = yield from self.redis.hsetnx(key, field, value)
        self.assertEqual(test_value, 1)
        # make sure that value was stored
        result = yield from self.redis.hget(key, field)
        self.assertEqual(result, value)
        # field exists, operation should not change any value
        test_value = yield from self.redis.hsetnx(key, field, b'baz')
        self.assertEqual(test_value, 0)
        # make sure value was not changed
        result = yield from self.redis.hget(key, field)
        self.assertEqual(result, value)

        with self.assertRaises(TypeError):
            yield from self.redis.hsetnx(None, field, value)

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

        test_value = yield from self.redis.hvals(key, encoding='utf-8')
        self.assertEqual(set(test_value), {'baz', 'zap'})
        with self.assertRaises(TypeError):
            yield from self.redis.hvals(None)

    @unittest.skipIf(REDIS_VERSION < (2, 8, 0),
                     'HSCAN is available since redis>=2.8.0')
    @run_until_complete
    def test_hscan(self):
        key = b'key:hscan'
        for k in (yield from self.redis.keys(key+b'*')):
            self.redis.delete(k)
        # setup initial values 3 "field:foo:*" items and 7 "field:bar:*" items
        for i in range(1, 11):
            foo_or_bar = 'bar' if i % 3 else 'foo'
            f = 'field:{}:{}'.format(foo_or_bar, i).encode('utf-8')
            v = 'value:{}'.format(i).encode('utf-8')
            yield from self.add(key, f, v)
        # fetch 'field:foo:*' items expected tuple with 3 fields and 3 values
        cursor, values = yield from self.redis.hscan(key, match=b'field:foo:*')
        self.assertEqual(len(values), 3*2)
        # fetch 'field:bar:*' items expected tuple with 7 fields and 7 values
        cursor, values = yield from self.redis.hscan(key, match=b'field:bar:*')
        self.assertEqual(len(values), 7*2)

        # SCAN family functions do not guarantee that the number of
        # elements returned per call are in a given range. So here
        # just dummy test, that *count* argument does not break something
        cursor = b'0'
        test_values = []
        while cursor:
            cursor, values = yield from self.redis.hscan(key, cursor, count=1)
            test_values.extend(values)
        self.assertEqual(len(test_values), 10*2)

        with self.assertRaises(TypeError):
            yield from self.redis.hscan(None)

    @unittest.skipUnless(PY_35, "Python 3.5+ required")
    @unittest.skipIf(REDIS_VERSION < (2, 8, 0),
                     'HSCAN is available since redis>=2.8.0')
    @run_until_complete
    def test_ihscan(self):
        key = b'key:hscan'
        for k in (yield from self.redis.keys(key+b'*')):
            self.redis.delete(k)
        # setup initial values 3 "field:foo:*" items and 7 "field:bar:*" items
        for i in range(1, 11):
            foo_or_bar = 'bar' if i % 3 else 'foo'
            f = 'field:{}:{}'.format(foo_or_bar, i).encode('utf-8')
            v = 'value:{}'.format(i).encode('utf-8')
            yield from self.add(key, f, v)

        s = dedent('''\
        async def coro(cmd):
            lst = []
            async for i in cmd:
                lst.append(i)
            return lst
        ''')
        lcl = {}
        exec(s, globals(), lcl)
        coro = lcl['coro']

        # fetch 'field:foo:*' items expected tuple with 3 fields and 3 values
        ret = yield from coro(self.redis.ihscan(key, match=b'field:foo:*'))
        self.assertEqual(set(ret), {(b'field:foo:3', b'value:3'),
                                    (b'field:foo:6', b'value:6'),
                                    (b'field:foo:9', b'value:9')})

        # fetch 'field:bar:*' items expected tuple with 7 fields and 7 values
        ret = yield from coro(self.redis.ihscan(key, match=b'field:bar:*'))
        self.assertEqual(set(ret), {(b'field:bar:1', b'value:1'),
                                    (b'field:bar:2', b'value:2'),
                                    (b'field:bar:4', b'value:4'),
                                    (b'field:bar:5', b'value:5'),
                                    (b'field:bar:7', b'value:7'),
                                    (b'field:bar:8', b'value:8'),
                                    (b'field:bar:10', b'value:10')})

        # SCAN family functions do not guarantee that the number of
        # elements returned per call are in a given range. So here
        # just dummy test, that *count* argument does not break something
        ret = yield from coro(self.redis.ihscan(key, count=1))
        self.assertEqual(set(ret), {(b'field:foo:3', b'value:3'),
                                    (b'field:foo:6', b'value:6'),
                                    (b'field:foo:9', b'value:9'),
                                    (b'field:bar:1', b'value:1'),
                                    (b'field:bar:2', b'value:2'),
                                    (b'field:bar:4', b'value:4'),
                                    (b'field:bar:5', b'value:5'),
                                    (b'field:bar:7', b'value:7'),
                                    (b'field:bar:8', b'value:8'),
                                    (b'field:bar:10', b'value:10')})

        with self.assertRaises(TypeError):
            yield from self.redis.ihscan(None)


class HashCommandsEncodingTest(RedisEncodingTest):
    @run_until_complete
    def test_hgetall(self):
        TEST_KEY = 'my-key-nx'
        yield from self.redis._conn.execute('MULTI')

        res = yield from self.redis.hgetall(TEST_KEY)
        self.assertEqual(res, 'QUEUED')

        yield from self.redis._conn.execute('EXEC')
