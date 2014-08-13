from ._testutil import RedisTest, run_until_complete
from aioredis import ReplyError


class TransactionCommandsTest(RedisTest):

    @run_until_complete
    def test_watch_unwatch(self):
        res = yield from self.redis.watch('key')
        self.assertTrue(res)
        res = yield from self.redis.watch('key', 'key')
        self.assertTrue(res)

        with self.assertRaises(TypeError):
            yield from self.redis.watch(None)
        with self.assertRaises(TypeError):
            yield from self.redis.watch('key', None)
        with self.assertRaises(TypeError):
            yield from self.redis.watch('key', 'key', None)

        res = yield from self.redis.unwatch()
        self.assertTrue(res)

    @run_until_complete
    def test_multi(self):
        res = yield from self.redis.multi()
        self.assertTrue(res)
        res = yield from self.redis.set('key', 'val')
        self.assertEqual(res, b'QUEUED')
        res = yield from self.redis.exec()
        self.assertEqual(res, [b'OK'])

        yield from self.redis.multi()
        with self.assertRaises(AssertionError):
            yield from self.redis.multi()
        res = yield from self.redis.discard()
        self.assertTrue(res)

        yield from self.redis.multi()
        with self.assertRaises(ReplyError):
            yield from self.redis.connection.execute('MULTI')
        res = yield from self.redis.discard()
        self.assertTrue(res)

    @run_until_complete
    def test_exec(self):
        yield from self.redis.multi()
        yield from self.redis.connection.execute('INCRBY', 'foo', '1.0')
        res = yield from self.redis.exec(return_exceptions=True)
        self.assertIsInstance(res[0], ReplyError)

        with self.assertRaises(ReplyError):
            yield from self.redis.multi()
            yield from self.redis.connection.execute('INCRBY', 'foo', '1.0')
            yield from self.redis.exec()
        with self.assertRaises(AssertionError):
            yield from self.redis.exec()
        with self.assertRaises(ReplyError):
            yield from self.redis.connection.execute('EXEC')

    @run_until_complete
    def test_discard(self):
        with self.assertRaises(AssertionError):
            yield from self.redis.discard()
        with self.assertRaises(ReplyError):
            yield from self.redis.connection.execute('DISCARD')
