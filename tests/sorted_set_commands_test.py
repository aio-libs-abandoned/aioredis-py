import itertools
import unittest

from ._testutil import RedisTest, run_until_complete, REDIS_VERSION


class SortedSetsCommandsTest(RedisTest):

    @run_until_complete
    def test_zadd(self):
        key = b'key:zadd'
        res = yield from self.redis.zadd(key, 1, b'one')
        self.assertEqual(res, 1)
        res = yield from self.redis.zadd(key, 1, b'one')
        self.assertEqual(res, 0)
        res = yield from self.redis.zadd(key, 1, b'uno')
        self.assertEqual(res, 1)
        res = yield from self.redis.zadd(key, 2.5, b'two')
        self.assertEqual(res, 1)

        res = yield from self.redis.zadd(key, 3, b'three', 4, b'four')
        self.assertEqual(res, 2)
        res = yield from self.redis.zrange(key, 0, -1, withscores=False)
        self.assertEqual(res, [b'one', b'uno', b'two', b'three', b'four'])

        with self.assertRaises(TypeError):
            yield from self.redis.zadd(None, 1, b'one')
        with self.assertRaises(TypeError):
            yield from self.redis.zadd(key, b'two', b'one')
        with self.assertRaises(TypeError):
            yield from self.redis.zadd(key, 3, b'three', 4)
        with self.assertRaises(TypeError):
            yield from self.redis.zadd(key, 3, b'three', 'four', 4)

    @run_until_complete
    def test_zcard(self):
        key = b'key:zcard'
        pairs = [1, b'one', 2, b'two', 3, b'three']
        res = yield from self.redis.zadd(key, *pairs)
        self.assertEqual(res, 3)
        res = yield from self.redis.zcard(key)
        self.assertEqual(res, 3)
        res = yield from self.redis.zadd(key, 1, b'ein')
        self.assertEqual(res, 1)
        res = yield from self.redis.zcard(key)
        self.assertEqual(res, 4)

        with self.assertRaises(TypeError):
            yield from self.redis.zcard(None)

    @run_until_complete
    def test_zcount(self):
        key = b'key:zcount'
        pairs = [1, b'one', 1, b'uno', 2.5, b'two', 3, b'three', 7, b'seven']
        res = yield from self.redis.zadd(key, *pairs)
        self.assertEqual(res, 5)

        res_zcount = yield from self.redis.zcount(key)
        res_zcard = yield from self.redis.zcard(key)
        self.assertEqual(res_zcount, res_zcard)

        res = yield from self.redis.zcount(key, 1, 3)
        self.assertEqual(res, 4)
        res = yield from self.redis.zcount(key, 3, 10)
        self.assertEqual(res, 2)
        res = yield from self.redis.zcount(key, 100, 200)
        self.assertEqual(res, 0)

        res = yield from self.redis.zcount(
            key, 1, 3, exclude=self.redis.ZSET_EXCLUDE_BOTH)
        self.assertEqual(res, 1)
        res = yield from self.redis.zcount(
            key, 1, 3, exclude=self.redis.ZSET_EXCLUDE_MIN)
        self.assertEqual(res, 2)
        res = yield from self.redis.zcount(
            key, 1, 3, exclude=self.redis.ZSET_EXCLUDE_MAX)
        self.assertEqual(res, 3)
        res = yield from self.redis.zcount(
            key, 1, exclude=self.redis.ZSET_EXCLUDE_MAX)
        self.assertEqual(res, 5)
        res = yield from self.redis.zcount(
            key, float('-inf'), 3, exclude=self.redis.ZSET_EXCLUDE_MIN)
        self.assertEqual(res, 4)

        with self.assertRaises(TypeError):
            yield from self.redis.zcount(None)
        with self.assertRaises(TypeError):
            yield from self.redis.zcount(key, 'one', 2)
        with self.assertRaises(TypeError):
            yield from self.redis.zcount(key, 1.1, b'two')
        with self.assertRaises(ValueError):
            yield from self.redis.zcount(key, 10, 1)

    @run_until_complete
    def test_zincrby(self):
        key = b'key:zincrby'
        pairs = [1, b'one', 1, b'uno', 2.5, b'two', 3, b'three']
        res = yield from self.redis.zadd(key, *pairs)
        res = yield from self.redis.zincrby(key, 1, b'one')
        self.assertEqual(res, 2)
        res = yield from self.redis.zincrby(key, -5, b'uno')
        self.assertEqual(res, -4)
        res = yield from self.redis.zincrby(key, 3.14, b'two')
        self.assertAlmostEqual(res, 5.64, delta=0.00001)
        res = yield from self.redis.zincrby(key, -3.14, b'three')
        self.assertAlmostEqual(res, -0.14, delta=0.00001)

        with self.assertRaises(TypeError):
            yield from self.redis.zincrby(None, 5, 'one')
        with self.assertRaises(TypeError):
            yield from self.redis.zincrby(key, 'one', 5)

    @run_until_complete
    def test_zinterstore(self):
        zset1 = [2, 'one', 2, 'two']
        zset2 = [3, 'one', 3, 'three']

        yield from self.redis.zadd('zset1', *zset1)
        yield from self.redis.zadd('zset2', *zset2)

        res = yield from self.redis.zinterstore('zout', 'zset1', 'zset2')
        self.assertEqual(res, 1)
        res = yield from self.redis.zrange('zout', withscores=True)
        self.assertEqual(res, [b'one', 5])

        res = yield from self.redis.zinterstore(
            'zout', 'zset1', 'zset2',
            aggregate=self.redis.ZSET_AGGREGATE_SUM)
        self.assertEqual(res, 1)
        res = yield from self.redis.zrange('zout', withscores=True)
        self.assertEqual(res, [b'one', 5])

        res = yield from self.redis.zinterstore(
            'zout', 'zset1', 'zset2',
            aggregate=self.redis.ZSET_AGGREGATE_MIN)
        self.assertEqual(res, 1)
        res = yield from self.redis.zrange('zout', withscores=True)
        self.assertEqual(res, [b'one', 2])

        res = yield from self.redis.zinterstore(
            'zout', 'zset1', 'zset2',
            aggregate=self.redis.ZSET_AGGREGATE_MAX)
        self.assertEqual(res, 1)
        res = yield from self.redis.zrange('zout', withscores=True)
        self.assertEqual(res, [b'one', 3])

        # weights

        with self.assertRaises(AssertionError):
            yield from self.redis.zinterstore('zout', 'zset1', 'zset2',
                                              with_weights=True)

        res = yield from self.redis.zinterstore('zout',
                                                ('zset1', 2), ('zset2', 2),
                                                with_weights=True)
        self.assertEqual(res, 1)
        res = yield from self.redis.zrange('zout', withscores=True)
        self.assertEqual(res, [b'one', 10])

    @unittest.skipIf(REDIS_VERSION < (2, 8, 9),
                     'ZLEXCOUNT is available since redis>=2.8.9')
    @run_until_complete
    def test_zlexcount(self):
        key = b'key:zlexcount'
        pairs = [0, b'a', 0, b'b', 0, b'c', 0, b'd', 0, b'e']
        res = yield from self.redis.zadd(key, *pairs)
        self.assertEqual(res, 5)
        res = yield from self.redis.zlexcount(key)
        self.assertEqual(res, 5)
        res = yield from self.redis.zlexcount(key, min=b'-', max=b'e')
        self.assertEqual(res, 5)
        res = yield from self.redis.zlexcount(key, min=b'a', max=b'e',
                                              include_min=False,
                                              include_max=False)
        self.assertEqual(res, 3)

        with self.assertRaises(TypeError):
            yield from self.redis.zlexcount(None, b'a', b'e')
        with self.assertRaises(TypeError):
            yield from self.redis.zlexcount(key, 10, b'e')
        with self.assertRaises(TypeError):
            yield from self.redis.zlexcount(key, b'a', 20)

    @run_until_complete
    def test_zrange(self):
        key = b'key:zrange'
        scores = [1, 1, 2.5, 3, 7]
        members = [b'one', b'uno', b'two', b'three', b'seven']
        pairs = list(itertools.chain(*zip(scores, members)))
        rev_pairs = list(itertools.chain(*zip(members, scores)))

        res = yield from self.redis.zadd(key, *pairs)
        self.assertEqual(res, 5)

        res = yield from self.redis.zrange(key, 0, -1, withscores=False)
        self.assertEqual(res, members)
        res = yield from self.redis.zrange(key, 0, -1, withscores=True)
        self.assertEqual(res, rev_pairs)
        res = yield from self.redis.zrange(key, -2, -1, withscores=False)
        self.assertEqual(res, members[-2:])
        res = yield from self.redis.zrange(key, 1, 2, withscores=False)
        self.assertEqual(res, members[1:3])
        with self.assertRaises(TypeError):
            yield from self.redis.zrange(None, 1, b'one')
        with self.assertRaises(TypeError):
            yield from self.redis.zrange(key, b'first', -1)
        with self.assertRaises(TypeError):
            yield from self.redis.zrange(key, 0, 'last')

    @unittest.skipIf(REDIS_VERSION < (2, 8, 9),
                     'ZRANGEBYLEX is available since redis>=2.8.9')
    @run_until_complete
    def test_zrangebylex(self):
        key = b'key:zrangebylex'
        scores = [0]*5
        members = [b'a', b'b', b'c', b'd', b'e']
        pairs = list(itertools.chain(*zip(scores, members)))

        res = yield from self.redis.zadd(key, *pairs)
        self.assertEqual(res, 5)
        res = yield from self.redis.zrangebylex(key)
        self.assertEqual(res, members)
        res = yield from self.redis.zrangebylex(key, min=b'-', max=b'd')
        self.assertEqual(res, members[:-1])
        res = yield from self.redis.zrangebylex(key, min=b'a', max=b'e',
                                                include_min=False,
                                                include_max=False)
        self.assertEqual(res, members[1:-1])
        res = yield from self.redis.zrangebylex(key, min=b'x', max=b'z')
        self.assertEqual(res, [])
        res = yield from self.redis.zrangebylex(key, min=b'e', max=b'a')
        self.assertEqual(res, [])
        res = yield from self.redis.zrangebylex(key, offset=1, count=2)
        self.assertEqual(res, members[1:3])
        with self.assertRaises(TypeError):
            yield from self.redis.zrangebylex(None, b'a', b'e')
        with self.assertRaises(TypeError):
            yield from self.redis.zrangebylex(key, 10, b'e')
        with self.assertRaises(TypeError):
            yield from self.redis.zrangebylex(key, b'a', 20)
        with self.assertRaises(TypeError):
            yield from self.redis.zrangebylex(key, b'a', b'e', offset=1)
        with self.assertRaises(TypeError):
            yield from self.redis.zrangebylex(key, b'a', b'e', count=1)
        with self.assertRaises(TypeError):
            yield from self.redis.zrangebylex(key, b'a', b'e',
                                              offset='one', count=1)
        with self.assertRaises(TypeError):
            yield from self.redis.zrangebylex(key, b'a', b'e',
                                              offset=1, count='one')

    @run_until_complete
    def test_zrank(self):
        key = b'key:zrank'
        scores = [1, 1, 2.5, 3, 7]
        members = [b'one', b'uno', b'two', b'three', b'seven']
        pairs = list(itertools.chain(*zip(scores, members)))

        res = yield from self.redis.zadd(key, *pairs)
        self.assertEqual(res, 5)

        for i, m in enumerate(members):
            res = yield from self.redis.zrank(key, m)
            self.assertEqual(res, i)

        res = yield from self.redis.zrank(key, b'not:exists')
        self.assertEqual(res, None)

        with self.assertRaises(TypeError):
            yield from self.redis.zrank(None, b'one')

    @run_until_complete
    def test_zrangebyscore(self):
        key = b'key:zrangebyscore'
        scores = [1, 1, 2.5, 3, 7]
        members = [b'one', b'uno', b'two', b'three', b'seven']
        pairs = list(itertools.chain(*zip(scores, members)))
        rev_pairs = list(itertools.chain(*zip(members, scores)))
        res = yield from self.redis.zadd(key, *pairs)
        self.assertEqual(res, 5)
        res = yield from self.redis.zrangebyscore(key, 1, 7, withscores=False)
        self.assertEqual(res, members)
        res = yield from self.redis.zrangebyscore(
            key, 1, 7, withscores=False, exclude=self.redis.ZSET_EXCLUDE_BOTH)
        self.assertEqual(res, members[2:-1])
        res = yield from self.redis.zrangebyscore(key, 1, 7, withscores=True)
        self.assertEqual(res, rev_pairs)

        res = yield from self.redis.zrangebyscore(key, 1, 10, offset=2,
                                                  count=2)
        self.assertEqual(res, members[2:4])

        with self.assertRaises(TypeError):
            yield from self.redis.zrangebyscore(None, 1, 7)
        with self.assertRaises(TypeError):
            yield from self.redis.zrangebyscore(key, 10, b'e')
        with self.assertRaises(TypeError):
            yield from self.redis.zrangebyscore(key, b'a', 20)
        with self.assertRaises(TypeError):
            yield from self.redis.zrangebyscore(key, 1, 7, offset=1)
        with self.assertRaises(TypeError):
            yield from self.redis.zrangebyscore(key, 1, 7, count=1)
        with self.assertRaises(TypeError):
            yield from self.redis.zrangebyscore(key, 1, 7, offset='one',
                                                count=1)
        with self.assertRaises(TypeError):
            yield from self.redis.zrangebyscore(key, 1, 7, offset=1,
                                                count='one')

    @run_until_complete
    def test_zrem(self):
        key = b'key:zrem'
        scores = [1, 1, 2.5, 3, 7]
        members = [b'one', b'uno', b'two', b'three', b'seven']
        pairs = list(itertools.chain(*zip(scores, members)))

        res = yield from self.redis.zadd(key, *pairs)
        self.assertEqual(res, 5)

        res = yield from self.redis.zrem(key, b'uno', b'one')
        self.assertEqual(res, 2)

        res = yield from self.redis.zrange(key, 0, -1)
        self.assertEqual(res, members[2:])

        res = yield from self.redis.zrem(key, b'not:exists')
        self.assertEqual(res, 0)

        res = yield from self.redis.zrem(b'not:' + key, b'not:exists')
        self.assertEqual(res, 0)

        with self.assertRaises(TypeError):
            yield from self.redis.zrem(None, b'one')

    @unittest.skipIf(REDIS_VERSION < (2, 8, 9),
                     'ZREMRANGEBYLEX is available since redis>=2.8.9')
    @run_until_complete
    def test_zremrangebylex(self):
        key = b'key:zremrangebylex'
        members = [b'aaaa', b'b', b'c', b'd', b'e', b'foo', b'zap', b'zip',
                   b'ALPHA', b'alpha']
        scores = [0]*len(members)

        pairs = list(itertools.chain(*zip(scores, members)))
        res = yield from self.redis.zadd(key, *pairs)
        self.assertEqual(res, 10)

        res = yield from self.redis.zremrangebylex(key, b'alpha', b'omega',
                                                   include_max=True,
                                                   include_min=True)
        self.assertEqual(res, 6)
        res = yield from self.redis.zrange(key, 0, -1)
        self.assertEqual(res, [b'ALPHA', b'aaaa', b'zap', b'zip'])

        res = yield from self.redis.zremrangebylex(key, b'zap', b'zip',
                                                   include_max=False,
                                                   include_min=False)
        self.assertEqual(res, 0)

        res = yield from self.redis.zrange(key, 0, -1)
        self.assertEqual(res, [b'ALPHA', b'aaaa', b'zap', b'zip'])

        res = yield from self.redis.zremrangebylex(key)
        self.assertEqual(res, 4)
        res = yield from self.redis.zrange(key, 0, -1)
        self.assertEqual(res, [])

        with self.assertRaises(TypeError):
            yield from self.redis.zremrangebylex(None, b'a', b'e')
        with self.assertRaises(TypeError):
            yield from self.redis.zremrangebylex(key, 10, b'e')
        with self.assertRaises(TypeError):
            yield from self.redis.zremrangebylex(key, b'a', 20)

    @run_until_complete
    def test_zremrangebyrank(self):
        key = b'key:zremrangebyrank'
        scores = [0, 1, 2, 3, 4, 5]
        members = [b'zero', b'one', b'two', b'three', b'four', b'five']
        pairs = list(itertools.chain(*zip(scores, members)))
        res = yield from self.redis.zadd(key, *pairs)
        self.assertEqual(res, 6)

        res = yield from self.redis.zremrangebyrank(key, 0, 1)
        self.assertEqual(res, 2)
        res = yield from self.redis.zrange(key, 0, -1)
        self.assertEqual(res, members[2:])

        res = yield from self.redis.zremrangebyrank(key, -2, -1)
        self.assertEqual(res, 2)
        res = yield from self.redis.zrange(key, 0, -1)
        self.assertEqual(res, members[2:-2])

        with self.assertRaises(TypeError):
            yield from self.redis.zremrangebyrank(None, 1, 2)
        with self.assertRaises(TypeError):
            yield from self.redis.zremrangebyrank(key, b'first', -1)
        with self.assertRaises(TypeError):
            yield from self.redis.zremrangebyrank(key, 0, 'last')

    @run_until_complete
    def test_zremrangebyscore(self):
        key = b'key:zremrangebyscore'
        scores = [1, 1, 2.5, 3, 7]
        members = [b'one', b'uno', b'two', b'three', b'seven']
        pairs = list(itertools.chain(*zip(scores, members)))
        res = yield from self.redis.zadd(key, *pairs)
        self.assertEqual(res, 5)

        res = yield from self.redis.zremrangebyscore(
            key, 3, 7.5, exclude=self.redis.ZSET_EXCLUDE_MIN)
        self.assertEqual(res, 1)
        res = yield from self.redis.zrange(key, 0, -1)
        self.assertEqual(res, members[:-1])

        res = yield from self.redis.zremrangebyscore(
            key, 1, 3, exclude=self.redis.ZSET_EXCLUDE_BOTH)
        self.assertEqual(res, 1)
        res = yield from self.redis.zrange(key, 0, -1)
        self.assertEqual(res, [b'one', b'uno', b'three'])

        res = yield from self.redis.zremrangebyscore(key)
        self.assertEqual(res, 3)
        res = yield from self.redis.zrange(key, 0, -1)
        self.assertEqual(res, [])

        with self.assertRaises(TypeError):
            yield from self.redis.zremrangebyscore(None, 1, 2)
        with self.assertRaises(TypeError):
            yield from self.redis.zremrangebyscore(key, b'first', -1)
        with self.assertRaises(TypeError):
            yield from self.redis.zremrangebyscore(key, 0, 'last')

    @run_until_complete
    def test_zrevrange(self):
        key = b'key:zrevrange'
        scores = [1, 1, 2.5, 3, 7]
        members = [b'one', b'uno', b'two', b'three', b'seven']
        pairs = list(itertools.chain(*zip(scores, members)))

        res = yield from self.redis.zadd(key, *pairs)
        self.assertEqual(res, 5)

        res = yield from self.redis.zrevrange(key, 0, -1, withscores=False)
        self.assertEqual(res, members[::-1])
        res = yield from self.redis.zrevrange(key, 0, -1, withscores=True)
        self.assertEqual(res, pairs[::-1])
        res = yield from self.redis.zrevrange(key, -2, -1, withscores=False)
        self.assertEqual(res,  members[1::-1])
        res = yield from self.redis.zrevrange(key, 1, 2, withscores=False)

        self.assertEqual(res, members[3:1:-1])
        with self.assertRaises(TypeError):
            yield from self.redis.zrevrange(None, 1, b'one')
        with self.assertRaises(TypeError):
            yield from self.redis.zrevrange(key, b'first', -1)
        with self.assertRaises(TypeError):
            yield from self.redis.zrevrange(key, 0, 'last')

    @run_until_complete
    def test_zrevrank(self):
        key = b'key:zrevrank'
        scores = [1, 1, 2.5, 3, 7]
        members = [b'one', b'uno', b'two', b'three', b'seven']
        pairs = list(itertools.chain(*zip(scores, members)))

        res = yield from self.redis.zadd(key, *pairs)
        self.assertEqual(res, 5)

        for i, m in enumerate(members):
            res = yield from self.redis.zrevrank(key, m)
            self.assertEqual(res, len(members) - i - 1)

        res = yield from self.redis.zrevrank(key, b'not:exists')
        self.assertEqual(res, None)

        with self.assertRaises(TypeError):
            yield from self.redis.zrevrank(None, b'one')

    @run_until_complete
    def test_zscore(self):
        key = b'key:zscore'
        scores = [1, 1, 2.5, 3, 7]
        members = [b'one', b'uno', b'two', b'three', b'seven']
        pairs = list(itertools.chain(*zip(scores, members)))

        res = yield from self.redis.zadd(key, *pairs)
        self.assertEqual(res, 5)

        for s, m in zip(scores, members):
            res = yield from self.redis.zscore(key, m)
            self.assertEqual(res, s)
        with self.assertRaises(TypeError):
            yield from self.redis.zscore(None, b'one')
        # Check None on undefined members
        res = yield from self.redis.zscore(key, "undefined")
        self.assertEqual(res, None)

    @run_until_complete
    def test_zunionstore(self):
        zset1 = [2, 'one', 2, 'two']
        zset2 = [3, 'one', 3, 'three']

        yield from self.redis.zadd('zset1', *zset1)
        yield from self.redis.zadd('zset2', *zset2)

        res = yield from self.redis.zunionstore('zout', 'zset1', 'zset2')
        self.assertEqual(res, 3)
        res = yield from self.redis.zrange('zout', withscores=True)
        self.assertEqual(res, [b'two', 2, b'three', 3, b'one', 5])

        res = yield from self.redis.zunionstore(
            'zout', 'zset1', 'zset2',
            aggregate=self.redis.ZSET_AGGREGATE_SUM)
        self.assertEqual(res, 3)
        res = yield from self.redis.zrange('zout', withscores=True)
        self.assertEqual(res, [b'two', 2, b'three', 3, b'one', 5])

        res = yield from self.redis.zunionstore(
            'zout', 'zset1', 'zset2',
            aggregate=self.redis.ZSET_AGGREGATE_MIN)
        self.assertEqual(res, 3)
        res = yield from self.redis.zrange('zout', withscores=True)
        self.assertEqual(res, [b'one', 2, b'two', 2, b'three', 3])

        res = yield from self.redis.zunionstore(
            'zout', 'zset1', 'zset2',
            aggregate=self.redis.ZSET_AGGREGATE_MAX)
        self.assertEqual(res, 3)
        res = yield from self.redis.zrange('zout', withscores=True)
        self.assertEqual(res, [b'two', 2, b'one', 3, b'three', 3])

        # weights

        with self.assertRaises(AssertionError):
            yield from self.redis.zunionstore('zout', 'zset1', 'zset2',
                                              with_weights=True)

        res = yield from self.redis.zunionstore('zout',
                                                ('zset1', 2), ('zset2', 2),
                                                with_weights=True)
        self.assertEqual(res, 3)
        res = yield from self.redis.zrange('zout', withscores=True)
        self.assertEqual(res, [b'two', 4, b'three', 6, b'one', 10])

    @run_until_complete
    def test_zrevrangebyscore(self):
        key = b'key:zrevrangebyscore'
        scores = [1, 1, 2.5, 3, 7]
        members = [b'one', b'uno', b'two', b'three', b'seven']
        pairs = list(itertools.chain(*zip(scores, members)))
        rev_pairs = list(itertools.chain(*zip(members[::-1], scores[::-1])))
        res = yield from self.redis.zadd(key, *pairs)
        self.assertEqual(res, 5)
        res = yield from self.redis.zrevrangebyscore(key, 7, 1,
                                                     withscores=False)
        self.assertEqual(res, members[::-1])
        res = yield from self.redis.zrevrangebyscore(
            key, 7, 1, withscores=False,
            exclude=self.redis.ZSET_EXCLUDE_BOTH)
        self.assertEqual(res, members[-2:1:-1])
        res = yield from self.redis.zrevrangebyscore(key, 7, 1,
                                                     withscores=True)
        self.assertEqual(res, rev_pairs)

        res = yield from self.redis.zrevrangebyscore(key, 10, 1, offset=2,
                                                     count=2)
        self.assertEqual(res, members[-3:-5:-1])

        with self.assertRaises(TypeError):
            yield from self.redis.zrevrangebyscore(None, 1, 7)
        with self.assertRaises(TypeError):
            yield from self.redis.zrevrangebyscore(key, 10, b'e')
        with self.assertRaises(TypeError):
            yield from self.redis.zrevrangebyscore(key, b'a', 20)
        with self.assertRaises(TypeError):
            yield from self.redis.zrevrangebyscore(key, 1, 7, offset=1)
        with self.assertRaises(TypeError):
            yield from self.redis.zrevrangebyscore(key, 1, 7, count=1)
        with self.assertRaises(TypeError):
            yield from self.redis.zrevrangebyscore(key, 1, 7, offset='one',
                                                   count=1)
        with self.assertRaises(TypeError):
            yield from self.redis.zrevrangebyscore(key, 1, 7, offset=1,
                                                   count='one')

    @unittest.skipIf(REDIS_VERSION < (2, 8, 0),
                     'ZSCAN is available since redis>=2.8.0')
    @run_until_complete
    def test_zscan(self):
        key = b'key:zscan'
        scores, members = [], []

        for i in range(1, 11):
            foo_or_bar = 'bar' if i % 3 else 'foo'
            members.append('zmem:{}:{}'.format(foo_or_bar, i).encode('utf-8'))
            scores.append(i)
        pairs = list(itertools.chain(*zip(scores, members)))
        rev_pairs = list(itertools.chain(*zip(members, scores)))
        yield from self.redis.zadd(key, *pairs)

        cursor, values = yield from self.redis.zscan(key, match=b'zmem:foo:*')
        self.assertEqual(len(values), 3*2)

        cursor, values = yield from self.redis.zscan(key, match=b'zmem:bar:*')
        self.assertEqual(len(values), 7*2)

        # SCAN family functions do not guarantee that the number (count) of
        # elements returned per call are in a given range. So here
        # just dummy test, that *count* argument does not break something
        cursor = b'0'
        test_values = []
        while cursor:
            cursor, values = yield from self.redis.zscan(key, cursor, count=2)
            test_values.extend(values)
        self.assertEqual(test_values, rev_pairs)

        with self.assertRaises(TypeError):
            yield from self.redis.zscan(None)
