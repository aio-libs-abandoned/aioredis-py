import itertools
from ._testutil import RedisTest, run_until_complete


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

        res = yield from self.redis.zcount(key, 1, 3, include_min=False)
        self.assertEqual(res, 2)
        res = yield from self.redis.zcount(key, 1, 3, include_max=False)
        self.assertEqual(res, 3)
        res = yield from self.redis.zcount(key, 1, include_max=False)
        self.assertEqual(res, 5)
        res = yield from self.redis.zcount(key, float(b'-inf'), 3,
                                           include_min=False)
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
        res = yield from self.redis.zrangebyscore(key, 1, 7, withscores=False,
                                                  include_min=False,
                                                  include_max=False)
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
