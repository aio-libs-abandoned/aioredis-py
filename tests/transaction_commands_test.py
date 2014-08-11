import asyncio

from ._testutil import RedisTest, run_until_complete
from aioredis import ReplyError


class TransactionCommandsTest(RedisTest):

    @run_until_complete
    def test_multi_exec(self):
        yield from self.redis.delete('foo', 'bar')

        tr = self.redis.multi_exec()
        f1 = tr.incr('foo')
        f2 = tr.incr('bar')
        res = yield from tr.execute()
        self.assertEqual(res, [1, 1])
        res2 = yield from asyncio.gather(f1, f2, loop=self.loop)
        self.assertEqual(res, res2)

        with self.assertRaises(AssertionError):
            tr.incr('foo')
        with self.assertRaises(AssertionError):
            yield from tr.execute()

        tr = self.redis.multi_exec()
        f1 = tr.incr('foo')
        f2 = tr.incr('bar')
        yield from tr.execute()
        self.assertEqual((yield from f1), 2)
        self.assertEqual((yield from f2), 2)

        with self.assertRaises(TypeError):
            yield from self.redis.multi_exec().execute()

    # @run_until_complete
    # def test_multi_exec__conn_closed(self):
    #     with self.assertRaises(ReplyError):
    #         yield from self.redis.multi_exec(
    #             self.redis.incr('key'))

    @run_until_complete
    def test_multi_exec__discard(self):
        with self.assertRaises(ReplyError):
            tr = self.redis.multi_exec()
            fut = tr.connection.execute('MULTI')
            yield from tr.execute()
        with self.assertRaises(ReplyError):
            yield from fut

    @run_until_complete
    def test_multi_exec__exec_error(self):
        with self.assertRaises(ReplyError):
            tr = self.redis.multi_exec()
            fut = tr.connection.execute('INCRBY', 'key', '1.0')
            yield from tr.execute(return_exceptions=False)
        with self.assertRaises(ReplyError):
            yield from fut

        tr = self.redis.multi_exec()
        fut = tr.incrby('key', 1.0)
        with self.assertRaises(TypeError):
            yield from tr.execute(return_exceptions=False)
        with self.assertRaises(TypeError):
            yield from fut

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
