import asyncio

from ._testutil import RedisTest, run_until_complete
from aioredis import ReplyError


class ListCommandsTest(RedisTest):

    @run_until_complete
    def test_blpop(self):
        key1, value1 = b'key:blpop:1', b'blpop:value:1'
        key2, value2 = b'key:blpop:2', b'blpop:value:2'

        # setup list
        result = yield from self.redis.rpush(key1, value1, value2)
        self.assertEqual(result, 2)
        # make sure that left value poped
        test_value = yield from self.redis.blpop(key1)
        self.assertEqual(test_value, [key1, value1])
        # pop remaining value, so list should become empty
        test_value = yield from self.redis.blpop(key1)
        self.assertEqual(test_value, [key1, value2])

        with self.assertRaises(TypeError):
            yield from self.redis.blpop(None)
        with self.assertRaises(TypeError):
            yield from self.redis.blpop(key1, None)
        with self.assertRaises(TypeError):
            yield from self.redis.blpop(key1, timeout=b'one')
        with self.assertRaises(ValueError):
            yield from self.redis.blpop(key2, timeout=-10)

    @run_until_complete
    def test_blpop_blocking_features(self):
        key1, key2 = b'key:blpop:1', b'key:blpop:2'
        value = b'blpop:value:2'

        other_redis = yield from self.create_redis(
            ('localhost', self.redis_port), loop=self.loop)

        # create blocking task in separate connection
        consumer = other_redis.blpop(key1, key2)

        producer_task = asyncio.Task(
            self.push_data_with_sleep(key2, value), loop=self.loop)
        results = yield from asyncio.gather(
            consumer, producer_task, loop=self.loop)

        self.assertEqual(results[0], [key2, value])
        self.assertEqual(results[1], 1)

        # wait for data with timeout, list is emtpy, so blpop should
        # return None in 1 sec
        waiter = self.redis.blpop(key1, key2, timeout=1)
        test_value = yield from waiter
        self.assertEqual(test_value, None)
        other_redis.close()

    @asyncio.coroutine
    def push_data_with_sleep(self, key, *values):
        yield from asyncio.sleep(0.2, loop=self.loop)
        result = yield from self.redis.lpush(key, *values)
        return result

    @run_until_complete
    def test_brpop(self):
        key1, value1 = b'key:brpop:1', b'brpop:value:1'
        key2, value2 = b'key:brpop:2', b'brpop:value:2'

        # setup list
        result = yield from self.redis.rpush(key1, value1, value2)
        self.assertEqual(result, 2)
        # make sure that right value poped
        test_value = yield from self.redis.brpop(key1)
        self.assertEqual(test_value, [key1, value2])
        # pop remaining value, so list should become empty
        test_value = yield from self.redis.brpop(key1)
        self.assertEqual(test_value, [key1, value1])

        with self.assertRaises(TypeError):
            yield from self.redis.brpop(None)
        with self.assertRaises(TypeError):
            yield from self.redis.brpop(key1, None)
        with self.assertRaises(TypeError):
            yield from self.redis.brpop(key1, timeout=b'one')
        with self.assertRaises(ValueError):
            yield from self.redis.brpop(key2, timeout=-10)

    @run_until_complete
    def test_brpop_blocking_features(self):
        key1, key2 = b'key:brpop:1', b'key:brpop:2'
        value = b'brpop:value:2'

        other_redis = yield from self.create_redis(
            ('localhost', self.redis_port), loop=self.loop)
        # create blocking task in separate connection
        consumer_task = other_redis.brpop(key1, key2)

        producer_task = asyncio.Task(
            self.push_data_with_sleep(key2, value), loop=self.loop)

        results = yield from asyncio.gather(
            consumer_task, producer_task, loop=self.loop)

        self.assertEqual(results[0], [key2, value])
        self.assertEqual(results[1], 1)

        # wait for data with timeout, list is emtpy, so brpop should
        # return None in 1 sec
        waiter = self.redis.brpop(key1, key2, timeout=1)
        test_value = yield from waiter
        self.assertEqual(test_value, None)
        other_redis.close()

    @run_until_complete
    def test_brpoplpush(self):
        key = b'key:brpoplpush:1'
        value1, value2 = b'brpoplpush:value:1', b'brpoplpush:value:2'

        destkey = b'destkey:brpoplpush:1'

        # setup list
        yield from self.redis.rpush(key, value1, value2)

        # move value in into head of new list
        result = yield from self.redis.brpoplpush(key, destkey)
        self.assertEqual(result, value2)
        # move last value
        result = yield from self.redis.brpoplpush(key, destkey)
        self.assertEqual(result, value1)

        # make sure that all values stored in new destkey list
        test_value = yield from self.redis.lrange(destkey, 0, -1)
        self.assertEqual(test_value, [value1, value2])

        with self.assertRaises(TypeError):
            yield from self.redis.brpoplpush(None, destkey)

        with self.assertRaises(TypeError):
            yield from self.redis.brpoplpush(key, None)

        with self.assertRaises(TypeError):
            yield from self.redis.brpoplpush(key, destkey, timeout=b'one')

        with self.assertRaises(ValueError):
            yield from self.redis.brpoplpush(key, destkey, timeout=-10)

    @run_until_complete
    def test_brpoplpush_blocking_features(self):
        source = b'key:brpoplpush:12'
        value = b'brpoplpush:value:2'
        destkey = b'destkey:brpoplpush:2'
        other_redis = yield from self.create_redis(
            ('localhost', self.redis_port), loop=self.loop)
        # create blocking task
        consumer_task = other_redis.brpoplpush(source, destkey)
        producer_task = asyncio.Task(
            self.push_data_with_sleep(source, value), loop=self.loop)
        results = yield from asyncio.gather(
            consumer_task, producer_task, loop=self.loop)
        self.assertEqual(results[0], value)
        self.assertEqual(results[1], 1)

        # make sure that all values stored in new destkey list
        test_value = yield from self.redis.lrange(destkey, 0, -1)
        self.assertEqual(test_value, [value])

        # wait for data with timeout, list is emtpy, so brpoplpush should
        # return None in 1 sec
        waiter = self.redis.brpoplpush(source, destkey, timeout=1)
        test_value = yield from waiter
        self.assertEqual(test_value, None)
        other_redis.close()

    @run_until_complete
    def test_lindex(self):
        key, value = b'key:lindex:1', 'value:{}'
        # setup list
        values = [value.format(i).encode('utf-8') for i in range(0, 10)]
        yield from self.redis.rpush(key, *values)
        # make sure that all indexes are correct
        for i in range(0, 10):
            test_value = yield from self.redis.lindex(key, i)
            self.assertEqual(test_value, values[i])

        # get last element
        test_value = yield from self.redis.lindex(key, -1)
        self.assertEqual(test_value, b'value:9')

        # index of element if key does not exists
        test_value = yield from self.redis.lindex(b'not:' + key, 5)
        self.assertEqual(test_value, None)

        with self.assertRaises(TypeError):
            yield from self.redis.lindex(None, -1)

        with self.assertRaises(TypeError):
            yield from self.redis.lindex(key, b'one')

    @run_until_complete
    def test_linsert(self):
        key = b'key:linsert:1'
        value1, value2, value3, value4 = b'Hello', b'World', b'foo', b'bar'
        yield from self.redis.rpush(key, value1, value2)

        # insert element before pivot
        test_value = yield from self.redis.linsert(
            key, value2, value3, before=True)
        self.assertEqual(test_value, 3)
        # insert element after pivot
        test_value = yield from self.redis.linsert(
            key, value2, value4, before=False)
        self.assertEqual(test_value, 4)

        # make sure that values actually inserted in right placed
        test_value = yield from self.redis.lrange(key, 0, -1)
        expected = [value1, value3, value2, value4]
        self.assertEqual(test_value, expected)

        # try to insert something when pivot value does not exits
        test_value = yield from self.redis.linsert(
            key, b'not:pivot', value3, before=True)
        self.assertEqual(test_value, -1)

        with self.assertRaises(TypeError):
            yield from self.redis.linsert(None, value1, value3)

    @run_until_complete
    def test_llen(self):
        key = b'key:llen:1'
        value1, value2 = b'Hello', b'World'
        yield from self.redis.rpush(key, value1, value2)

        test_value = yield from self.redis.llen(key)
        self.assertEqual(test_value, 2)

        test_value = yield from self.redis.llen(b'not:' + key)
        self.assertEqual(test_value, 0)

        with self.assertRaises(TypeError):
            yield from self.redis.llen(None)

    @run_until_complete
    def test_lpop(self):
        key = b'key:lpop:1'
        value1, value2 = b'lpop:value:1', b'lpop:value:2'

        # setup list
        result = yield from self.redis.rpush(key, value1, value2)
        self.assertEqual(result, 2)
        # make sure that left value poped
        test_value = yield from self.redis.lpop(key)
        self.assertEqual(test_value, value1)
        # pop remaining value, so list should become empty
        test_value = yield from self.redis.lpop(key)
        self.assertEqual(test_value, value2)
        # pop from empty list
        test_value = yield from self.redis.lpop(key)
        self.assertEqual(test_value, None)

        with self.assertRaises(TypeError):
            yield from self.redis.lpop(None)

    @run_until_complete
    def test_lpush(self):
        key = b'key:lpush'
        value1, value2 = b'value:1', b'value:2'

        # add multiple values to the list, with key that does not exists
        result = yield from self.redis.lpush(key, value1, value2)
        self.assertEqual(result, 2)

        # make sure that values actually inserted in right placed and order
        test_value = yield from self.redis.lrange(key, 0, -1)
        self.assertEqual(test_value, [value2, value1])

        with self.assertRaises(TypeError):
            yield from self.redis.lpush(None, value1)

    @run_until_complete
    def test_lpushx(self):
        key = b'key:lpushx'
        value1, value2 = b'value:1', b'value:2'

        # add multiple values to the list, with key that does not exists
        # so value should not be pushed
        result = yield from self.redis.lpushx(key, value2)
        self.assertEqual(result, 0)
        # init key with list by using regular lpush
        result = yield from self.redis.lpush(key, value1)
        self.assertEqual(result, 1)

        result = yield from self.redis.lpushx(key, value2)
        self.assertEqual(result, 2)

        # make sure that values actually inserted in right placed and order
        test_value = yield from self.redis.lrange(key, 0, -1)
        self.assertEqual(test_value, [value2, value1])

        with self.assertRaises(TypeError):
            yield from self.redis.lpushx(None, value1)

    @run_until_complete
    def test_lrange(self):
        key, value = b'key:lrange:1', 'value:{}'
        values = [value.format(i).encode('utf-8') for i in range(0, 10)]
        yield from self.redis.rpush(key, *values)

        test_value = yield from self.redis.lrange(key, 0, 2)
        self.assertEqual(test_value, values[0:3])

        test_value = yield from self.redis.lrange(key, 0, -1)
        self.assertEqual(test_value, values)

        test_value = yield from self.redis.lrange(key, -2, -1)
        self.assertEqual(test_value, values[-2:])

        # range of elements if key does not exists
        test_value = yield from self.redis.lrange(b'not:' + key, 0, -1)
        self.assertEqual(test_value, [])

        with self.assertRaises(TypeError):
            yield from self.redis.lrange(None, 0, -1)

        with self.assertRaises(TypeError):
            yield from self.redis.lrange(key, b'zero', -1)

        with self.assertRaises(TypeError):
            yield from self.redis.lrange(key, 0, b'one')

    @run_until_complete
    def test_lrem(self):
        key, value = b'key:lrem:1', 'value:{}'
        values = [value.format(i % 2).encode('utf-8') for i in range(0, 10)]
        yield from self.redis.rpush(key, *values)
        # remove elements from tail to head
        test_value = yield from self.redis.lrem(key, -4, b'value:0')
        self.assertEqual(test_value, 4)
        # remove element from head to tail
        test_value = yield from self.redis.lrem(key, 4, b'value:1')
        self.assertEqual(test_value, 4)

        # remove values that not in list
        test_value = yield from self.redis.lrem(key, 4, b'value:other')
        self.assertEqual(test_value, 0)

        # make sure that only two values left in the list
        test_value = yield from self.redis.lrange(key, 0, -1)
        self.assertEqual(test_value, [b'value:0', b'value:1'])

        # remove all instance of value:0
        test_value = yield from self.redis.lrem(key, 0, b'value:0')
        self.assertEqual(test_value, 1)

        # make sure that only one values left in the list
        test_value = yield from self.redis.lrange(key, 0, -1)
        self.assertEqual(test_value, [b'value:1'])

        with self.assertRaises(TypeError):
            yield from self.redis.lrem(None, 0, b'value:0')

        with self.assertRaises(TypeError):
            yield from self.redis.lrem(key, b'ten', b'value:0')

    @run_until_complete
    def test_lset(self):
        key, value = b'key:lset', 'value:{}'
        values = [value.format(i).encode('utf-8') for i in range(0, 3)]
        yield from self.redis.rpush(key, *values)

        yield from self.redis.lset(key, 0, b'foo')
        yield from self.redis.lset(key, -1, b'baz')
        yield from self.redis.lset(key, -2, b'zap')

        test_value = yield from self.redis.lrange(key, 0, -1)
        self.assertEqual(test_value, [b'foo', b'zap', b'baz'])

        with self.assertRaises(TypeError):
            yield from self.redis.lset(None, 0, b'value:0')

        with self.assertRaises(ReplyError):
            yield from self.redis.lset(key, 100, b'value:0')

        with self.assertRaises(TypeError):
            yield from self.redis.lset(key, b'one', b'value:0')

    @run_until_complete
    def test_ltrim(self):
        key, value = b'key:ltrim', 'value:{}'
        values = [value.format(i).encode('utf-8') for i in range(0, 10)]
        yield from self.redis.rpush(key, *values)

        # trim with negative indexes
        yield from self.redis.ltrim(key, 0, -5)
        test_value = yield from self.redis.lrange(key, 0, -1)
        self.assertEqual(test_value, values[:-4])
        # trim with positive indexes
        yield from self.redis.ltrim(key, 0, 2)
        test_value = yield from self.redis.lrange(key, 0, -1)
        self.assertEqual(test_value, values[:3])

        # try to trim out of range indexes
        yield from self.redis.ltrim(key, 100, 110)
        test_value = yield from self.redis.lrange(key, 0, -1)
        self.assertEqual(test_value, [])

        with self.assertRaises(TypeError):
            yield from self.redis.ltrim(None, 0, -1)

        with self.assertRaises(TypeError):
            yield from self.redis.ltrim(key, b'zero', -1)

        with self.assertRaises(TypeError):
            yield from self.redis.ltrim(key, 0, b'one')

    @run_until_complete
    def test_rpop(self):
        key = b'key:rpop:1'
        value1, value2 = b'rpop:value:1', b'rpop:value:2'

        # setup list
        result = yield from self.redis.rpush(key, value1, value2)
        self.assertEqual(result, 2)
        # make sure that left value poped
        test_value = yield from self.redis.rpop(key)
        self.assertEqual(test_value, value2)
        # pop remaining value, so list should become empty
        test_value = yield from self.redis.rpop(key)
        self.assertEqual(test_value, value1)
        # pop from empty list
        test_value = yield from self.redis.rpop(key)
        self.assertEqual(test_value, None)

        with self.assertRaises(TypeError):
            yield from self.redis.rpop(None)

    @run_until_complete
    def test_rpoplpush(self):
        key = b'key:rpoplpush:1'
        value1, value2 = b'rpoplpush:value:1', b'rpoplpush:value:2'
        destkey = b'destkey:rpoplpush:1'

        # setup list
        yield from self.redis.rpush(key, value1, value2)

        # move value in into head of new list
        result = yield from self.redis.rpoplpush(key, destkey)
        self.assertEqual(result, value2)
        # move last value
        result = yield from self.redis.rpoplpush(key, destkey)
        self.assertEqual(result, value1)

        # make sure that all values stored in new destkey list
        test_value = yield from self.redis.lrange(destkey, 0, -1)
        self.assertEqual(test_value, [value1, value2])

        with self.assertRaises(TypeError):
            yield from self.redis.rpoplpush(None, destkey)

        with self.assertRaises(TypeError):
            yield from self.redis.rpoplpush(key, None)

    @run_until_complete
    def test_rpush(self):
        key = b'key:rpush'
        value1, value2 = b'value:1', b'value:2'

        # add multiple values to the list, with key that does not exists
        result = yield from self.redis.rpush(key, value1, value2)
        self.assertEqual(result, 2)

        # make sure that values actually inserted in right placed and order
        test_value = yield from self.redis.lrange(key, 0, -1)
        self.assertEqual(test_value, [value1, value2])

        with self.assertRaises(TypeError):
            yield from self.redis.rpush(None, value1)

    @run_until_complete
    def test_rpushx(self):
        key = b'key:rpushx'
        value1, value2 = b'value:1', b'value:2'

        # add multiple values to the list, with key that does not exists
        # so value should not be pushed
        result = yield from self.redis.rpushx(key, value2)
        self.assertEqual(result, 0)
        # init key with list by using regular rpush
        result = yield from self.redis.rpush(key, value1)
        self.assertEqual(result, 1)

        result = yield from self.redis.rpushx(key, value2)
        self.assertEqual(result, 2)

        # make sure that values actually inserted in right placed and order
        test_value = yield from self.redis.lrange(key, 0, -1)
        self.assertEqual(test_value, [value1, value2])

        with self.assertRaises(TypeError):
            yield from self.redis.rpushx(None, value1)
