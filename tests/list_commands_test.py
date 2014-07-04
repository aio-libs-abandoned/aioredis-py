import asyncio

from ._testutil import BaseTest, run_until_complete
from aioredis import create_redis, ReplyError


class ListCommandsTest(BaseTest):

    def setUp(self):
        super().setUp()
        self.redis = self.loop.run_until_complete(
            create_redis(('localhost', self.redis_port), loop=self.loop))

    def tearDown(self):
        self.redis.close()
        del self.redis
        super().tearDown()

    @run_until_complete
    def test_blpop(self):
        key1, value1 = b'key:blpop:1', b'blpop:value:1'
        key2, value2 = b'key:blpop:2', b'blpop:value:2'

        # setup list
        result = yield from self.redis.rpush(key1, value1)
        self.assertEqual(result, 1)
        result = yield from self.redis.rpush(key1, value2)
        self.assertEqual(result, 2)
        # make sure that left value poped
        test_value = yield from self.redis.blpop(key1)
        self.assertEqual(test_value, [key1, value1])
        # pop remaining value, so list should become empty
        test_value = yield from self.redis.blpop(key1)
        self.assertEqual(test_value, [key1, value2])


        # create one more redis connection for blocking operation
        self.other_redis = create_redis(
            'localhost', self.redis_port, loop=self.loop)

        # call *blpop*, and wait until value in list would be available
        waiter = asyncio.Task(self.redis.blpop(key1), loop=self.loop)
        # let's put something to list
        yield from self.redis.lpush(key1, value1)
        # value was added to list so lets wait blpop to return
        test_value = yield from waiter
        self.assertEqual(test_value, [key1, value1])

        # lets wait for data in two separate lists
        waiter = asyncio.Task(self.redis.blpop(key1, key2), loop=self.loop)
        # supply data to second list
        yield from self.redis.lpush(key2, value2)
        # wait blpop to return
        test_value = yield from waiter
        self.assertEqual(test_value, [key2, value2])

        # wait for data with timeout, list is emtpy, so blpop should
        # return None in 1 sec
        waiter = asyncio.Task(
            self.redis.blpop(key1, key2, timeout=1), loop=self.loop)
        test_value = yield from waiter
        self.assertEqual(test_value, None)

        with self.assertRaises(TypeError):
            yield from self.redis.blpop(None)
        self.other_redis.close()

    @run_until_complete
    def test_brpop(self):
        key1, value1 = b'key:brpop:1', b'brpop:value:1'
        key2, value2 = b'key:brpop:2', b'brpop:value:2'

        # setup list
        result = yield from self.redis.rpush(key1, value1)
        self.assertEqual(result, 1)
        result = yield from self.redis.rpush(key1, value2)
        self.assertEqual(result, 2)
        # make sure that right value poped
        test_value = yield from self.redis.brpop(key1)
        self.assertEqual(test_value, [key1, value2])
        # pop remaining value, so list should become empty
        test_value = yield from self.redis.brpop(key1)
        self.assertEqual(test_value, [key1, value1])


        # create one more redis connection for blocking operation
        self.other_redis = create_redis(
            'localhost', self.redis_port, loop=self.loop)

        # call *brpop*, and wait until value in list would be available
        waiter = asyncio.Task(self.redis.brpop(key1), loop=self.loop)
        # let's put something to list
        yield from self.redis.lpush(key1, value1)
        # value was added to list so lets wait brpop to return
        test_value = yield from waiter
        self.assertEqual(test_value, [key1, value1])

        # lets wait for data in two separate lists
        waiter = asyncio.Task(self.redis.brpop(key1, key2), loop=self.loop)
        # supply data to second list
        yield from self.redis.lpush(key2, value2)
        # wait brpop to return
        test_value = yield from waiter
        self.assertEqual(test_value, [key2, value2])

        # wait for data with timeout, list is emtpy, so brpop should
        # return None in 1 sec
        waiter = asyncio.Task(
            self.redis.brpop(key1, key2, timeout=1), loop=self.loop)
        test_value = yield from waiter
        self.assertEqual(test_value, None)

        with self.assertRaises(TypeError):
            yield from self.redis.brpop(None)
        self.other_redis.close()

    @run_until_complete
    def test_brpoplpush(self):
        key1, value1 = b'key:brpoplpush:1', b'brpoplpush:value:1'
        key2, value2 = b'key:brpoplpush:2', b'brpoplpush:value:2'

        destkey = b'destkey:brpoplpush:1'

        # setup list
        yield from self.redis.rpush(key1, value1)
        yield from self.redis.rpush(key1, value2)

        # move value in into head of new list
        result = yield from self.redis.brpoplpush(key1, destkey)
        self.assertEqual(result, value2)
        # move last value
        result = yield from self.redis.brpoplpush(key1, destkey)
        self.assertEqual(result, value1)

        # make sure that all values stored in new destkey list
        test_value = yield from self.redis.lrange(destkey, 0,-1)
        self.assertEqual(test_value, [value1, value2])

        # lets test blocking features of this command
        # create one more redis connection for blocking operation
        self.other_redis = create_redis(
            'localhost', self.redis_port, loop=self.loop)

        # call *brpoplpush*, and wait until value in list would be available
        waiter = asyncio.Task(self.redis.brpoplpush(key1, destkey), loop=self.loop)
        # let's put something to list
        yield from self.redis.lpush(key1, value1)
        # value was added to list so lets wait brpoplpush to return
        test_value = yield from waiter
        self.assertEqual(test_value, value1)

        # return None in 1 sec
        waiter = asyncio.Task(
            self.redis.brpoplpush(key1, destkey, timeout=1), loop=self.loop)
        test_value = yield from waiter
        self.assertEqual(test_value, None)

        with self.assertRaises(TypeError):
            yield from self.redis.brpoplpush(None, destkey)

        with self.assertRaises(TypeError):
            yield from  self.redis.brpoplpush(key1, None)
        self.other_redis.close()


    @run_until_complete
    def test_lindex(self):
        key, value = b'key:lindex:1', 'value:{}'

        # setup list
        for i in range(0, 10):
            _value = value.format(i).encode('utf-8')
            test_value = yield from self.redis.rpush(key, _value)
            self.assertEqual(test_value, i+1)

        # make sure that all indexes are correct
        for i in range(0, 10):
            test_value = yield from self.redis.lindex(key, i)
            _value = value.format(i).encode('utf-8')
            self.assertEqual(test_value, _value)

        # get last element
        test_value = yield from self.redis.lindex(key, -1)
        self.assertEqual(test_value, b'value:9')

        # index of element if key does not exists
        test_value = yield from self.redis.lindex(b'not:' + key, 5)
        self.assertEqual(test_value, None)

        with self.assertRaises(TypeError):
            yield from self.redis.lindex(None, -1)


