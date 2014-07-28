import asyncio
from ._testutil import RedisTest, run_until_complete
from aioredis import ReplyError


class StringCommandsTest(RedisTest):

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

        with self.assertRaises(TypeError):
            yield from self.redis.bitcount(None, 2, 2)
        with self.assertRaises(TypeError):
            yield from self.redis.bitcount('my-key', None, 2)
        with self.assertRaises(TypeError):
            yield from self.redis.bitcount('my-key', 2, None)

    @run_until_complete
    def test_bitop_and(self):
        key1, value1 = b'key:bitop:and:1', 5
        key2, value2 = b'key:bitop:and:2', 7

        yield from self.add(key1, value1)
        yield from self.add(key2, value2)

        destkey = b'key:bitop:dest'

        yield from self.redis.bitop_and(destkey, key1, key2)
        test_value = yield from self.redis.get(destkey)
        self.assertEqual(test_value, b'5')

        with self.assertRaises(TypeError):
            yield from self.redis.bitop_and(None, key1, key2)
        with self.assertRaises(TypeError):
            yield from self.redis.bitop_and(destkey, None)
        with self.assertRaises(TypeError):
            yield from self.redis.bitop_and(destkey, key1, None)

    @run_until_complete
    def test_bitop_or(self):
        key1, value1 = b'key:bitop:or:1', 5
        key2, value2 = b'key:bitop:or:2', 7

        yield from self.add(key1, value1)
        yield from self.add(key2, value2)

        destkey = b'key:bitop:dest'

        yield from self.redis.bitop_or(destkey, key1, key2)
        test_value = yield from self.redis.get(destkey)
        self.assertEqual(test_value, b'7')

        with self.assertRaises(TypeError):
            yield from self.redis.bitop_or(None, key1, key2)
        with self.assertRaises(TypeError):
            yield from self.redis.bitop_or(destkey, None)
        with self.assertRaises(TypeError):
            yield from self.redis.bitop_or(destkey, key1, None)

    @run_until_complete
    def test_bitop_xor(self):
        key1, value1 = b'key:bitop:xor:1', 5
        key2, value2 = b'key:bitop:xor:2', 7

        yield from self.add(key1, value1)
        yield from self.add(key2, value2)

        destkey = b'key:bitop:dest'

        yield from self.redis.bitop_xor(destkey, key1, key2)
        test_value = yield from self.redis.get(destkey)
        self.assertEqual(test_value, b'\x02')

        with self.assertRaises(TypeError):
            yield from self.redis.bitop_xor(None, key1, key2)
        with self.assertRaises(TypeError):
            yield from self.redis.bitop_xor(destkey, None)
        with self.assertRaises(TypeError):
            yield from self.redis.bitop_xor(destkey, key1, None)

    @run_until_complete
    def test_bitop_not(self):
        key1, value1 = b'key:bitop:not:1', 5
        yield from self.add(key1, value1)

        destkey = b'key:bitop:dest'

        yield from self.redis.bitop_not(destkey, key1)
        res = yield from self.redis.get(destkey)
        self.assertEqual(res, b'\xca')

        with self.assertRaises(TypeError):
            yield from self.redis.bitop_not(None, key1)
        with self.assertRaises(TypeError):
            yield from self.redis.bitop_not(destkey, None)

    @run_until_complete
    def test_bitpos(self):
        key, value = b'key:bitop', b'\xff\xf0\x00'
        yield from self.add(key, value)
        test_value = yield from self.redis.bitpos(key, 0, end=3)
        self.assertEqual(test_value, 12)

        test_value = yield from self.redis.bitpos(key, 0, 2, 3)
        self.assertEqual(test_value, 16)

        key, value = b'key:bitop', b'\x00\xff\xf0'
        yield from self.add(key, value)
        test_value = yield from self.redis.bitpos(key, 1, 0)
        self.assertEqual(test_value, 8)

        test_value = yield from self.redis.bitpos(key, 1, 1)
        self.assertEqual(test_value, 8)

        key, value = b'key:bitop', b'\x00\x00\x00'
        yield from self.add(key, value)
        test_value = yield from self.redis.bitpos(key, 1, 0)
        self.assertEqual(test_value, -1)

        test_value = yield from self.redis.bitpos(b'not:' + key, 1)
        self.assertEqual(test_value, -1)

        with self.assertRaises(TypeError):
            test_value = yield from self.redis.bitpos(None, 1)

        with self.assertRaises(ValueError):
            test_value = yield from self.redis.bitpos(key, 7)

    @run_until_complete
    def test_decr(self):
        yield from self.redis.delete('key')

        res = yield from self.redis.decr('key')
        self.assertEqual(res, -1)
        res = yield from self.redis.decr('key')
        self.assertEqual(res, -2)

        with self.assertRaises(ReplyError):
            yield from self.add('key', 'val')
            yield from self.redis.decr('key')
        with self.assertRaises(ReplyError):
            yield from self.add('key', 1.0)
            yield from self.redis.decr('key')
        with self.assertRaises(TypeError):
            yield from self.redis.decr(None)

    @run_until_complete
    def test_decrby(self):
        yield from self.redis.delete('key')

        res = yield from self.redis.decrby('key', 1)
        self.assertEqual(res, -1)
        res = yield from self.redis.decrby('key', 10)
        self.assertEqual(res, -11)
        res = yield from self.redis.decrby('key', -1)
        self.assertEqual(res, -10)

        with self.assertRaises(ReplyError):
            yield from self.add('key', 'val')
            yield from self.redis.decrby('key', 1)
        with self.assertRaises(ReplyError):
            yield from self.add('key', 1.0)
            yield from self.redis.decrby('key', 1)
        with self.assertRaises(TypeError):
            yield from self.redis.decrby(None, 1)
        with self.assertRaises(TypeError):
            yield from self.redis.decrby('key', None)

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
    def test_getbit(self):
        key, value = b'key:getbit', 10
        yield from self.add(key, value)

        result = yield from self.redis.setbit(key, 7, 1)
        self.assertEqual(result, 1)

        test_value = yield from self.redis.getbit(key, 0)
        self.assertEqual(test_value, 0)

        test_value = yield from self.redis.getbit(key, 7)
        self.assertEqual(test_value, 1)

        test_value = yield from self.redis.getbit(b'not:' + key, 7)
        self.assertEqual(test_value, 0)

        test_value = yield from self.redis.getbit(key, 100)
        self.assertEqual(test_value, 0)

        with self.assertRaises(TypeError):
            yield from self.redis.getbit(None, 0)
        with self.assertRaises(TypeError):
            yield from self.redis.getbit(key, b'one')
        with self.assertRaises(ValueError):
            yield from self.redis.getbit(key, -7)

    @run_until_complete
    def test_getrange(self):
        key, value = b'key:getrange', b'This is a string'
        yield from self.add(key, value)

        test_value = yield from self.redis.getrange(key, 0, 3)
        self.assertEqual(test_value, b'This')

        test_value = yield from self.redis.getrange(key, -3, -1)
        self.assertEqual(test_value, b'ing')

        test_value = yield from self.redis.getrange(key, 0, -1)
        self.assertEqual(test_value, b'This is a string')

        test_value = yield from self.redis.getrange(key, 10, 100)
        self.assertEqual(test_value, b'string')

        test_value = yield from self.redis.getrange(key, 50, 100)
        self.assertEqual(test_value, b'')

        with self.assertRaises(TypeError):
            yield from self.redis.getrange(None, 0, 3)
        with self.assertRaises(TypeError):
            yield from self.redis.getrange(key, b'one', 3)
        with self.assertRaises(TypeError):
            yield from self.redis.getrange(key, 0, b'seven')

    @run_until_complete
    def test_getset(self):
        key, value = b'key:getset', b'hello'
        yield from self.add(key, value)

        test_value = yield from self.redis.getset(key, b'asyncio')
        self.assertEqual(test_value, b'hello')

        test_value = yield from self.redis.get(key)
        self.assertEqual(test_value, b'asyncio')

        test_value = yield from self.redis.getset(b'not:' + key, b'asyncio')
        self.assertEqual(test_value, None)

        test_value = yield from self.redis.get(b'not:' + key)
        self.assertEqual(test_value, b'asyncio')

        with self.assertRaises(TypeError):
            yield from self.redis.getset(None, b'asyncio')

    @run_until_complete
    def test_incr(self):
        yield from self.redis.delete('key')

        res = yield from self.redis.incr('key')
        self.assertEqual(res, 1)
        res = yield from self.redis.incr('key')
        self.assertEqual(res, 2)

        with self.assertRaises(ReplyError):
            yield from self.add('key', 'val')
            yield from self.redis.incr('key')
        with self.assertRaises(ReplyError):
            yield from self.add('key', 1.0)
            yield from self.redis.incr('key')
        with self.assertRaises(TypeError):
            yield from self.redis.incr(None)

    @run_until_complete
    def test_incrby(self):
        yield from self.redis.delete('key')

        res = yield from self.redis.incrby('key', 1)
        self.assertEqual(res, 1)
        res = yield from self.redis.incrby('key', 10)
        self.assertEqual(res, 11)
        res = yield from self.redis.incrby('key', -1)
        self.assertEqual(res, 10)

        with self.assertRaises(ReplyError):
            yield from self.add('key', 'val')
            yield from self.redis.incrby('key', 1)
        with self.assertRaises(ReplyError):
            yield from self.add('key', 1.0)
            yield from self.redis.incrby('key', 1)
        with self.assertRaises(TypeError):
            yield from self.redis.incrby(None, 1)
        with self.assertRaises(TypeError):
            yield from self.redis.incrby('key', None)

    @run_until_complete
    def test_incrbyfloat(self):
        yield from self.redis.delete('key')

        res = yield from self.redis.incrbyfloat('key', 1.0)
        self.assertEqual(res, 1.0)
        res = yield from self.redis.incrbyfloat('key', 10.5)
        self.assertEqual(res, 11.5)
        res = yield from self.redis.incrbyfloat('key', -1.0)
        self.assertEqual(res, 10.5)
        yield from self.add('key', 2)
        res = yield from self.redis.incrbyfloat('key', 0.5)
        self.assertEqual(res, 2.5)

        with self.assertRaises(ReplyError):
            yield from self.add('key', 'val')
            yield from self.redis.incrbyfloat('key', 1.0)
        with self.assertRaises(TypeError):
            yield from self.redis.incrbyfloat(None, 1.0)
        with self.assertRaises(TypeError):
            yield from self.redis.incrbyfloat('key', None)
        with self.assertRaises(TypeError):
            yield from self.redis.incrbyfloat('key', 1)
        with self.assertRaises(TypeError):
            yield from self.redis.incrbyfloat('key', '1.0')

    @run_until_complete
    def test_mget(self):
        yield from self.redis.connection.execute('flushall')
        key1, value1 = b'foo', b'bar'
        key2, value2 = b'baz', b'bzz'
        yield from self.add(key1, value1)
        yield from self.add(key2, value2)

        res = yield from self.redis.mget('key')
        self.assertEqual(res, [None])
        res = yield from self.redis.mget('key', 'key')
        self.assertEqual(res, [None, None])

        res = yield from self.redis.mget(key1, key2)
        self.assertEqual(res, [value1, value2])

        with self.assertRaises(TypeError):
            yield from self.redis.mget(None, key2)
        with self.assertRaises(TypeError):
            yield from self.redis.mget(key1, None)

    @run_until_complete
    def test_mset(self):
        key1, value1 = b'key:mset:1', b'hello'
        key2, value2 = b'key:mset:2', b'world'

        yield from self.redis.mset(key1, value1, key2, value2)

        test_value = yield from self.redis.mget(key1, key2)
        self.assertEqual(test_value, [value1, value2])

        yield from self.redis.mset(b'other:' + key1, b'other:' + value1)
        test_value = yield from self.redis.get(b'other:' + key1)
        self.assertEqual(test_value, b'other:' + value1)

        with self.assertRaises(TypeError):
            yield from self.redis.mset(None, value1)
        with self.assertRaises(TypeError):
            yield from self.redis.mset(key1, value1, key1)

    @run_until_complete
    def test_msetnx(self):
        key1, value1 = b'key:msetnx:1', b'Hello'
        key2, value2 = b'key:msetnx:2', b'there'
        key3, value3 = b'key:msetnx:3', b'world'

        res = yield from self.redis.msetnx(key1, value1, key2, value2)
        self.assertEqual(res, 1)
        res = yield from self.redis.mget(key1, key2)
        self.assertEqual(res, [value1, value2])
        res = yield from self.redis.msetnx(key2, value2, key3, value3)
        self.assertEqual(res, 0)
        res = yield from self.redis.mget(key1, key2, key3)
        self.assertEqual(res, [value1, value2, None])

        with self.assertRaises(TypeError):
            yield from self.redis.msetnx(None, value1)
        with self.assertRaises(TypeError):
            yield from self.redis.msetnx(key1, value1, key2)

    @run_until_complete
    def test_psetex(self):
        key, value = b'key:psetex:1', b'Hello'
        # test expiration in milliseconds
        yield from self.redis.psetex(key, 10, value)
        test_value = yield from self.redis.get(key)
        self.assertEqual(test_value, value)

        yield from asyncio.sleep(0.011, loop=self.loop)
        test_value = yield from self.redis.get(key)
        self.assertEqual(test_value, None)

        with self.assertRaises(TypeError):
            yield from self.redis.psetex(None, 10, value)
        with self.assertRaises(TypeError):
            yield from self.redis.psetex(key, 7.5, value)

    @run_until_complete
    def test_set(self):
        ok = yield from self.redis.set('my-key', 'value')
        self.assertEqual(ok, b'OK')

        with self.assertRaises(TypeError):
            yield from self.redis.set(None, 'value')

    @run_until_complete
    def test_set_expire(self):
        key, value = b'key:set:expire', b'foo'
        # test expiration in milliseconds
        yield from self.redis.set(key, value, pexpire=10)
        result_1 = yield from self.redis.get(key)
        self.assertEqual(result_1, value)
        yield from asyncio.sleep(0.011, loop=self.loop)
        result_2 = yield from self.redis.get(key)
        self.assertEqual(result_2, None)

        # same thing but timeout in seconds
        yield from self.redis.set(key, value, expire=1)
        result_3 = yield from self.redis.get(key)
        self.assertEqual(result_3, value)
        yield from asyncio.sleep(1.001, loop=self.loop)
        result_4 = yield from self.redis.get(key)
        self.assertEqual(result_4, None)

    @run_until_complete
    def test_set_only_if_not_exists(self):
        key, value = b'key:set:only_if_not_exists', b'foo'
        yield from self.redis.set(key, value, only_if_not_exists=True)
        result_1 = yield from self.redis.get(key)
        self.assertEqual(result_1, value)

        # new values not set cos, values exists
        yield from self.redis.set(key, "foo2", only_if_not_exists=True)
        result_2 = yield from self.redis.get(key)
        # nothing changed result is same "foo"
        self.assertEqual(result_2, value)

    @run_until_complete
    def test_set_only_if_exists(self):
        key, value = b'key:set:only_if_exists', b'only_if_exists:foo'
        # ensure that such key does not exits, and value not sets
        yield from self.redis.delete(key)
        yield from self.redis.set(key, value, only_if_exists=True)
        result_1 = yield from self.redis.get(key)
        self.assertEqual(result_1, None)

        # ensure key exits, and value updates
        yield from self.redis.set(key, value)
        yield from self.redis.set(key, b'foo', only_if_exists=True)
        result_2 = yield from self.redis.get(key)
        self.assertEqual(result_2, b'foo')

    @run_until_complete
    def test_set_wrong_input(self):
        key, value = b'key:set:', b'foo'

        with self.assertRaises(TypeError):
            yield from self.redis.set(None, value)
        with self.assertRaises(TypeError):
            yield from self.redis.set(key, value, expire=7.8)
        with self.assertRaises(TypeError):
            yield from self.redis.set(key, value, pexpire=7.8)
        with self.assertRaises(TypeError):
            yield from self.redis.set(key, value, only_if_not_exists=True,
                                      only_if_exists=True)

    @run_until_complete
    def test_setbit(self):
        key = b'key:setbit'
        result = yield from self.redis.setbit(key, 7, 1)
        self.assertEqual(result, 0)
        test_value = yield from self.redis.getbit(key, 7)
        self.assertEqual(test_value, 1)

        with self.assertRaises(TypeError):
            yield from self.redis.setbit(None, 7, 1)
        with self.assertRaises(TypeError):
            yield from self.redis.setbit(key, 7.5, 1)
        with self.assertRaises(ValueError):
            yield from self.redis.setbit(key, -1, 1)
        with self.assertRaises(ValueError):
            yield from self.redis.setbit(key, 1, 7)

    @run_until_complete
    def test_setex(self):
        key, value = b'key:setex:1', b'Hello'
        yield from self.redis.setex(key, 1, value)
        test_value = yield from self.redis.get(key)
        self.assertEqual(test_value, value)
        yield from asyncio.sleep(1, loop=self.loop)
        test_value = yield from self.redis.get(key)
        self.assertEqual(test_value, None)

        yield from self.redis.setex(key, 0.1, value)
        test_value = yield from self.redis.get(key)
        self.assertEqual(test_value, value)
        yield from asyncio.sleep(0.11, loop=self.loop)
        test_value = yield from self.redis.get(key)
        self.assertEqual(test_value, None)

        with self.assertRaises(TypeError):
            yield from self.redis.setex(None, 1, value)
        with self.assertRaises(TypeError):
            yield from self.redis.setex(key, b'one', value)

    @run_until_complete
    def test_setnx(self):
        key, value = b'key:setnx:1', b'Hello'
        # set fresh new value
        test_value = yield from self.redis.setnx(key, value)
        # 1 means value has been set
        self.assertEqual(test_value, 1)
        # fetch installed value just to be sure
        test_value = yield from self.redis.get(key)
        self.assertEqual(test_value, value)
        # try to set new value on same key
        test_value = yield from self.redis.setnx(key, b'other:' + value)
        # 0 means value has not been set
        self.assertEqual(test_value, 0)
        # make sure that value was not changed
        test_value = yield from self.redis.get(key)
        self.assertEqual(test_value, value)

        with self.assertRaises(TypeError):
            yield from self.redis.setnx(None, value)

    @run_until_complete
    def test_setrange(self):
        key, value = b'key:setrange', b'Hello World'
        yield from self.add(key, value)
        test_value = yield from self.redis.setrange(key, 6, b'Redis')
        self.assertEqual(test_value, 11)
        test_value = yield from self.redis.get(key)
        self.assertEqual(test_value, b'Hello Redis')

        test_value = yield from self.redis.setrange(b'not:' + key, 6, b'Redis')
        self.assertEqual(test_value, 11)
        test_value = yield from self.redis.get(b'not:' + key)
        self.assertEqual(test_value, b'\x00\x00\x00\x00\x00\x00Redis')

        with self.assertRaises(TypeError):
            yield from self.redis.setrange(None, 6, b'Redis')
        with self.assertRaises(TypeError):
            yield from self.redis.setrange(key, 0.7, b'Redis')
        with self.assertRaises(ValueError):
            yield from self.redis.setrange(key, -1, b'Redis')

    @run_until_complete
    def test_strlen(self):
        key, value = b'key:strlen', b'asyncio'
        yield from self.add(key, value)
        test_value = yield from self.redis.strlen(key)
        self.assertEqual(test_value, len(value))

        test_value = yield from self.redis.strlen(b'not:' + key)
        self.assertEqual(test_value, 0)

        with self.assertRaises(TypeError):
            yield from self.redis.strlen(None)
