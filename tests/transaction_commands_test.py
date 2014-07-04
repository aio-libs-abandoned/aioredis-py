
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
                self.redis.connection.execute('INCRBY', 'key', '1.0'))
