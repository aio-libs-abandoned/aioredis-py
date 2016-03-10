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
        self.assertEqual(res, [True, 2.2])
        res2 = yield from asyncio.gather(f1, f2, loop=self.loop)
        self.assertEqual(res, res2)

        tr = self.redis.multi_exec()
        f1 = tr.incrby('foo', 1.0)
        with self.assertRaisesRegex(MultiExecError,
                                    "increment must be .* int"):
            yield from tr.execute()
        with self.assertRaises(TypeError):
            yield from f1

    @run_until_complete
    def test_empty(self):
        tr = self.redis.multi_exec()
        res = yield from tr.execute()
        self.assertEqual(res, [])

    @run_until_complete
    def test_double_execute(self):
        tr = self.redis.multi_exec()
        yield from tr.execute()
        with self.assertRaises(AssertionError):
            yield from tr.execute()
        with self.assertRaises(AssertionError):
            yield from tr.incr('foo')

    @run_until_complete
    def test_connection_closed(self):
        tr = self.redis.multi_exec()
        fut1 = tr.quit()
        fut2 = tr.incrby('foo', 1.0)
        fut3 = tr.connection.execute('INCRBY', 'foo', '1.0')
        with self.assertRaises(MultiExecError):
            yield from tr.execute()

        self.assertTrue(fut1.done())
        self.assertTrue(fut2.done())
        self.assertTrue(fut3.done())

        try:
            res = yield from fut1
            self.assertEqual(res, b'OK')
        except asyncio.CancelledError:
            pass
        self.assertFalse(fut2.cancelled())
        self.assertIsInstance(fut2.exception(), TypeError)

        self.assertTrue(fut3.cancelled())

    @run_until_complete
    def test_discard(self):
        yield from self.redis.delete('foo')
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
        # with self.assertRaises(ReplyError):
        res = yield from fut3
        self.assertEqual(res, 1)

    @run_until_complete
    def test_exec_error(self):
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
    def test_command_errors(self):
        tr = self.redis.multi_exec()
        fut = tr.incrby('key', 1.0)
        with self.assertRaises(MultiExecError):
            yield from tr.execute()
        with self.assertRaises(TypeError):
            yield from fut

    @run_until_complete
    def test_several_command_errors(self):
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
    def test_error_in_connection(self):
        yield from self.redis.set('foo', 1)
        tr = self.redis.multi_exec()
        fut1 = tr.mget('foo', None)
        fut2 = tr.incr('foo')
        with self.assertRaises(MultiExecError):
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
    def test_encoding(self):
        res = yield from self.redis.set('key', 'value')
        self.assertTrue(res)
        res = yield from self.redis.hmset(
            'hash-key', 'foo', 'val1', 'bar', 'val2')
        self.assertTrue(res)

        tr = self.redis.multi_exec()
        fut1 = tr.get('key')
        fut2 = tr.get('key', encoding='utf-8')
        fut3 = tr.hgetall('hash-key', encoding='utf-8')
        yield from tr.execute()
        res = yield from fut1
        self.assertEqual(res, b'value')
        res = yield from fut2
        self.assertEqual(res, 'value')
        res = yield from fut3
        self.assertEqual(res, {'foo': 'val1', 'bar': 'val2'})

    @run_until_complete
    def test_global_encoding(self):
        redis = yield from self.create_redis(
            ('localhost', self.redis_port),
            loop=self.loop, encoding='utf-8')
        res = yield from redis.set('key', 'value')
        self.assertTrue(res)
        res = yield from redis.hmset(
            'hash-key', 'foo', 'val1', 'bar', 'val2')
        self.assertTrue(res)

        tr = redis.multi_exec()
        fut1 = tr.get('key')
        fut2 = tr.get('key', encoding='utf-8')
        fut3 = tr.hgetall('hash-key', encoding='utf-8')
        yield from tr.execute()
        res = yield from fut1
        self.assertEqual(res, 'value')
        res = yield from fut2
        self.assertEqual(res, 'value')
        res = yield from fut3
        self.assertEqual(res, {'foo': 'val1', 'bar': 'val2'})
