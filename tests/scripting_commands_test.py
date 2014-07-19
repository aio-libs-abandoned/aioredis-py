import asyncio

from ._testutil import RedisTest, run_until_complete
from aioredis import create_redis, ReplyError


class SetCommandsTest(RedisTest):

    @run_until_complete
    def test_eval(self):
        script = "return 42"
        res = yield from self.redis.eval(script)
        self.assertEqual(res, 42)

        key, value = b'key:eval', b'value:eval'
        script = """
        if redis.call('setnx', KEYS[1], ARGV[1]) == 1
        then
            return 'foo'
        else
            return 'bar'
        end
        """
        res = yield from self.redis.eval(script, keys=[key], args=[value])
        self.assertEqual(res, b'foo')
        res = yield from self.redis.eval(script, keys=[key], args=[value])
        self.assertEqual(res, b'bar')

        script = "return 42"
        with self.assertRaises(TypeError):
            yield from self.redis.eval(script, keys='not:list')

        with self.assertRaises(TypeError):
            yield from self.redis.eval(script, keys=['valid', None])
        with self.assertRaises(TypeError):
            yield from self.redis.eval(script, args=['valid', None])
        with self.assertRaises(TypeError):
            yield from self.redis.eval(None)

    @run_until_complete
    def test_evalsha(self):
        script = b"return 42"
        sha_hash = yield from self.redis.script_load(script)
        self.assertEqual(len(sha_hash), 40)
        res = yield from self.redis.evalsha(sha_hash)
        self.assertEqual(res, 42)

        key, arg1, arg2 = b'key:evalsha', b'1', b'2'
        script = "return {KEYS[1], ARGV[1], ARGV[2]}"
        sha_hash = yield from self.redis.script_load(script)
        res = yield from self.redis.evalsha(sha_hash, [key], [arg1, arg2])
        self.assertEqual(res, [key, arg1, arg2])

        with self.assertRaises(ValueError):
            yield from self.redis.evalsha(b'wrong sha hash')
        with self.assertRaises(TypeError):
            yield from self.redis.evalsha(sha_hash, keys=['valid', None])
        with self.assertRaises(TypeError):
            yield from self.redis.evalsha(sha_hash, args=['valid', None])
        with self.assertRaises(TypeError):
            yield from self.redis.evalsha(None)

    @run_until_complete
    def test_script_exists(self):
        sha_hash1 = yield from self.redis.script_load(b'return 1')
        sha_hash2 = yield from self.redis.script_load(b'return 2')
        self.assertEqual(len(sha_hash1), 40)
        self.assertEqual(len(sha_hash2), 40)

        res = yield from self.redis.script_exists(sha_hash1, sha_hash1)
        self.assertEqual(res, [1, 1])

        no_sha = b'ffffffffffffffffffffffffffffffffffffffff'
        res = yield from self.redis.script_exists(no_sha)
        self.assertEqual(res, [0])

        with self.assertRaises(ValueError):
            yield from self.redis.script_exists(b'wrong sha hash')
        with self.assertRaises(ValueError):
            yield from self.redis.script_exists(no_sha, b'wrong sha hash')

    @run_until_complete
    def test_script_flush(self):
        sha_hash1 = yield from self.redis.script_load(b'return 1')
        self.assertEqual(len(sha_hash1), 40)
        res = yield from self.redis.script_exists(sha_hash1)
        self.assertEqual(res, [1])
        res = yield from self.redis.script_flush()
        res = yield from self.redis.script_exists(sha_hash1)
        self.assertEqual(res, [0])

    @run_until_complete
    def test_script_load(self):
        sha_hash1 = yield from self.redis.script_load(b'return 1')
        sha_hash2 = yield from self.redis.script_load(b'return 2')
        self.assertEqual(len(sha_hash1), 40)
        self.assertEqual(len(sha_hash2), 40)
        res = yield from self.redis.script_exists(sha_hash1, sha_hash1)
        self.assertEqual(res, [1, 1])

    @run_until_complete
    def test_script_kill(self):
        script = "while (1) do redis.call('TIME') end"

        other_redis = yield from create_redis(
            ('localhost', self.redis_port), loop=self.loop)

        yield from self.add('key1', 'value')

        coro = other_redis.eval(script, keys=['non-existent-key'], args=[10])
        blocked_task = asyncio.Task(coro, loop=self.loop)
        yield from asyncio.sleep(0, loop=self.loop)
        resp = yield from self.redis.script_kill()
        self.assertEqual(resp, b'OK')

        with self.assertRaises(ReplyError):
            yield from blocked_task

        with self.assertRaises(ReplyError):
            yield from self.redis.script_kill()
