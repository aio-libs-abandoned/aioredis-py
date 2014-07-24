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
        yield from self.add('foo', 'bar')
        yield from self.add('baz', 'bzz')

        res = yield from self.redis.mget('key')
        self.assertEqual(res, [None])
        res = yield from self.redis.mget('key', 'key')
        self.assertEqual(res, [None, None])

        res = yield from self.redis.mget('foo', 'baz')
        self.assertEqual(res, [b'bar', b'bzz'])

    @run_until_complete
    def test_set(self):
        ok = yield from self.redis.set('my-key', 'value')
        self.assertEqual(ok, b'OK')

        with self.assertRaises(TypeError):
            yield from self.redis.set(None, 'value')
