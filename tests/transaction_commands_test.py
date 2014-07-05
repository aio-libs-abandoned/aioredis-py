
from ._testutil import RedisTest, run_until_complete
from aioredis import ReplyError


class TransactionCommandsTest(RedisTest):

    @run_until_complete
    def test_multi_exec(self):
        yield from self.redis.delete('foo', 'bar')

        res = yield from self.redis.multi_exec(
            self.redis.incr('foo'),
            self.redis.incr('bar'))
        self.assertEqual(res, [1, 1])
        res = yield from self.redis.multi_exec(
            self.redis.incr('foo'),
            self.redis.incr('bar'))
        self.assertEqual(res, [2, 2])

        with self.assertRaises(TypeError):
            yield from self.redis.multi_exec()
        with self.assertRaises(TypeError):
            yield from self.redis.multi_exec(self.redis.incr)

    @run_until_complete
    def test_multi_exec__conn_closed(self):
        with self.assertRaises(ReplyError):
            yield from self.redis.multi_exec(
                self.redis.set('key', None))

    @run_until_complete
    def test_multi_exec__discard(self):
        with self.assertRaises(ReplyError):
            yield from self.redis.multi_exec(
                self.redis.connection.execute('MULTI'))

    @run_until_complete
    def test_multi_exec__exec_error(self):
        with self.assertRaises(ReplyError):
            yield from self.redis.multi_exec(
                self.redis.connection.execute('INCRBY', 'key', '1.0'))

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
