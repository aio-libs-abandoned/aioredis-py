import asyncio

from ._testutil import RedisTest, run_until_complete
from aioredis import ReplyError, MultiExecError


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

        tr = self.redis.multi_exec()
        f1 = tr.set('foo', 1.0)
        f2 = tr.incrbyfloat('foo', 1.2)
        res = yield from tr.execute()
        self.assertEqual(res, [b'OK', 2.2])
        res2 = yield from asyncio.gather(f1, f2, loop=self.loop)
        self.assertEqual(res, res2)

        # with self.assertRaisesRegex(TypeError, "At least one command"):
        #     yield from self.redis.multi_exec().execute()
        tr = self.redis.multi_exec()
        tr.incrby('foo', 1.0)
        with self.assertRaisesRegex(TypeError, "increment must be .* int"):
            yield from tr.execute()

    @run_until_complete
    def test_multi_exec__conn_closed(self):
        tr = self.redis.multi_exec()
        fut1 = tr.quit()
        fut2 = tr.incrby('foo', 1.0)
        fut3 = tr.connection.execute('INCRBY', 'foo', '1.0')
        res = yield from tr.execute()
        self.assertIsNone(res)
        res = yield from fut1
        self.assertEqual(res, b'OK')
        with self.assertRaises(TypeError):
            yield from fut2
        with self.assertRaises(asyncio.CancelledError):
            yield from fut3

    @run_until_complete
    def test_multi_exec__discard(self):
        tr = self.redis.multi_exec()
        fut1 = tr.incrby('foo', 1.0)
        fut2 = tr.connection.execute('MULTI')
        fut3 = tr.connection.execute('incr', 'foo')

        with self.assertRaises(ReplyError):
            yield from tr.execute()
        with self.assertRaises(TypeError):
            yield from fut1
        with self.assertRaises(ReplyError):
            yield from fut2
        with self.assertRaises(ReplyError):
            yield from fut3

    @run_until_complete
    def test_multi_exec__exec_error(self):
        tr = self.redis.multi_exec()
        fut = tr.connection.execute('INCRBY', 'key', '1.0')
        with self.assertRaises(MultiExecError):
            yield from tr.execute()
        with self.assertRaises(ReplyError):
            yield from fut

        yield from self.redis.set('foo', 'bar')
        tr = self.redis.multi_exec()
        fut = tr.incrbyfloat('foo', 1.1)
        res = yield from tr.execute(return_exceptions=True)
        self.assertIsInstance(res[0], ReplyError)
        with self.assertRaises(ReplyError):
            yield from fut

    @run_until_complete
    def test_multi_exec__type_errors(self):
        tr = self.redis.multi_exec()
        fut = tr.incrby('key', 1.0)
        with self.assertRaises(TypeError):
            yield from tr.execute()
        with self.assertRaises(TypeError):
            yield from fut

    @run_until_complete
    def test_multi_exec__several_type_errors(self):
        tr = self.redis.multi_exec()
        fut1 = tr.incrby('key', 1.0)
        fut2 = tr.rename('bar', 'bar')
        with self.assertRaises(MultiExecError):
            yield from tr.execute()
        with self.assertRaises(TypeError):
            yield from fut1
        with self.assertRaises(ValueError):
            yield from fut2

    @run_until_complete
    def test_multi_exec__err_in_connection(self):
        yield from self.redis.set('foo', 1)
        tr = self.redis.multi_exec()
        fut1 = tr.mget('foo', None)
        fut2 = tr.incr('foo')
        with self.assertRaises(TypeError):
            yield from tr.execute()
        with self.assertRaises(TypeError):
            yield from fut1
        yield from fut2

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
