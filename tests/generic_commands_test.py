import asyncio
import time
import math
import unittest
from unittest import mock

from ._testutil import RedisTest, run_until_complete
from aioredis import create_redis


class GenericCommandsTest(RedisTest):

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

    @run_until_complete
    def test_wait_expire(self):
        return
        yield from self.add('my-key', 123)
        res = yield from self.redis.expire('my-key', 1)
        self.assertIs(res, True)

        yield from asyncio.sleep(1, loop=self.loop)

        res = yield from self.redis.exists('my-key')
        self.assertIs(res, False)

    @run_until_complete
    def test_wait_expireat(self):
        return
        yield from self.add('my-key', 123)
        ts = int(time.time() + 1)
        res = yield from self.redis.expireat('my-key', ts)

        yield from asyncio.sleep(ts - time.time(), loop=self.loop)
        res = yield from self.redis.exists('my-key')
        self.assertIs(res, False)

    @run_until_complete
    def test_expireat(self):
        yield from self.add('my-key', 123)
        now = math.ceil(time.time())

        res = yield from self.redis.expireat('my-key', now + 10)
        self.assertIs(res, True)

        res = yield from self.redis.connection.execute('TTL', 'my-key')
        self.assertGreaterEqual(res, 10)

        res = yield from self.redis.expireat('my-key', -1)
        self.assertIs(res, True)

        res = yield from self.redis.exists('my-key')
        self.assertIs(res, False)

        yield from self.add('my-key', 123)

        res = yield from self.redis.expireat('my-key', 0)
        self.assertIs(res, True)

        res = yield from self.redis.exists('my-key')
        self.assertIs(res, False)

        yield from self.add('my-key', 123)
        res = yield from self.redis.expireat('my-key', time.time() + 10)
        self.assertIs(res, True)

        res = yield from self.redis.connection.execute('TTL', 'my-key')
        self.assertGreaterEqual(res, 10)

        yield from self.add('my-key', 123)
        with self.assertRaises(TypeError):
            yield from self.redis.expireat(None, 123)
        with self.assertRaises(TypeError):
            yield from self.redis.expireat('my-key', 'timestamp')

    @run_until_complete
    def test_keys(self):
        res = yield from self.redis.keys('*pattern*')
        self.assertEqual(res, [])

        yield from self.redis.connection.execute('FLUSHDB')
        res = yield from self.redis.keys('*')
        self.assertEqual(res, [])

        yield from self.add('my-key-1', 1)
        yield from self.add('my-key-ab', 1)

        res = yield from self.redis.keys('my-key-?')
        self.assertEqual(res, [b'my-key-1'])
        res = yield from self.redis.keys('my-key-*')
        self.assertEqual(sorted(res), [b'my-key-1', b'my-key-ab'])

        with self.assertRaises(TypeError):
            yield from self.redis.keys(None)

    @run_until_complete
    @unittest.skip("Need another Redis instance")
    def test_migrate(self):
        yield from self.add('my-key', 123)

        conn2 = yield from create_redis(('localhost', 6380), db=2,
                                        loop=self.loop)
        yield from conn2.delete('my-key')
        self.assertTrue((yield from self.redis.exists('my-key')))
        self.assertFalse((yield from conn2.exists('my-key')))

        ok = yield from self.redis.migrate('localhost', 6380, 'my-key',
                                           2, 1000)
        self.assertTrue(ok)
        self.assertFalse((yield from self.redis.exists('my-key')))
        self.assertTrue((yield from conn2.exists('my-key')))

        with self.assertRaisesRegex(TypeError, "host .* str"):
            yield from self.redis.migrate(None, 1234, 'key', 1, 23)
        with self.assertRaisesRegex(TypeError, "key .* None"):
            yield from self.redis.migrate('host', '1234',  None, 1, 123)
        with self.assertRaisesRegex(TypeError, "dest_db .* int"):
            yield from self.redis.migrate('host', 123, 'key', 1.0, 123)
        with self.assertRaisesRegex(TypeError, "timeout .* int"):
            yield from self.redis.migrate('host', '1234', 'key', 2, None)
        with self.assertRaisesRegex(ValueError, "Got empty host"):
            yield from self.redis.migrate('', '123', 'key', 1, 123)
        with self.assertRaisesRegex(ValueError, "dest_db .* greater equal 0"):
            yield from self.redis.migrate('host', 6379, 'key', -1, 1000)
        with self.assertRaisesRegex(ValueError, "timeout .* greater equal 0"):
            yield from self.redis.migrate('host', 6379, 'key', 1, -1000)

    @run_until_complete
    def test_move(self):
        res = yield from self.redis.connection.execute('FLUSHALL')
        self.assertEqual(res, b'OK')

        yield from self.add('my-key', 123)

        self.assertEqual(self.redis.db, 0)
        res = yield from self.redis.move('my-key', 1)
        self.assertIs(res, True)

        with self.assertRaises(TypeError):
            yield from self.redis.move(None, 1)
        with self.assertRaises(TypeError):
            yield from self.redis.move('my-key', None)
        with self.assertRaises(ValueError):
            yield from self.redis.move('my-key', -1)
        with self.assertRaises(TypeError):
            yield from self.redis.move('my-key', 'not db')

    @unittest.skip('Not implemented')
    @run_until_complete
    def test_object(self):
        pass

    @run_until_complete
    def test_persist(self):
        yield from self.add('my-key', 123)
        res = yield from self.redis.expire('my-key', 10)
        self.assertTrue(res)

        res = yield from self.redis.persist('my-key')
        self.assertIs(res, True)

        res = yield from self.redis.connection.execute('TTL', 'my-key')
        self.assertEqual(res, -1)

        with self.assertRaises(TypeError):
            yield from self.redis.persist(None)

    @run_until_complete
    def test_pexpire(self):
        yield from self.add('my-key', 123)
        res = yield from self.redis.pexpire('my-key', 100)
        self.assertIs(res, True)

        res = yield from self.redis.connection.execute('TTL', 'my-key')
        self.assertEqual(res, 0)
        res = yield from self.redis.connection.execute('PTTL', 'my-key')
        self.assertGreater(res, 0)

        yield from self.add('my-key', 123)
        res = yield from self.redis.pexpire('my-key', 1)
        self.assertTrue(res)

        yield from asyncio.sleep(.002, loop=self.loop)

        res = yield from self.redis.exists('my-key')
        self.assertFalse(res)

        with self.assertRaises(TypeError):
            yield from self.redis.pexpire(None, 0)
        with self.assertRaises(TypeError):
            yield from self.redis.pexpire('my-key', 1.0)
