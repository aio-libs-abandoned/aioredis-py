import unittest

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

    @run_until_complete
    @unittest.expectedFailure
    def test_bitop_and(self):
        raise NotImplementedError

    @run_until_complete
    @unittest.expectedFailure
    def test_bitop_or(self):
        raise NotImplementedError

    @run_until_complete
    @unittest.expectedFailure
    def test_bitop_xor(self):
        raise NotImplementedError

    @run_until_complete
    @unittest.expectedFailure
    def test_bitop_not(self):
        raise NotImplementedError

    @run_until_complete
    @unittest.expectedFailure
    def test_bitpos(self):
        raise NotImplementedError

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
    @unittest.expectedFailure
    def test_getbit(self):
        raise NotImplementedError

    @run_until_complete
    @unittest.expectedFailure
    def test_getrange(self):
        raise NotImplementedError

    @run_until_complete
    @unittest.expectedFailure
    def test_getset(self):
        raise NotImplementedError

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
