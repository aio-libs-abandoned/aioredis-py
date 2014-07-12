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

        res = yield from self.redis.zadd(key, 3, b'tree', 4, b'four')
        self.assertEqual(res, 2)
        res = yield from self.redis.zrange(key, 0, -1, withscores=False)
        self.assertEqual(res, [b'one', b'uno', b'two', b'tree', b'four'])

        with self.assertRaises(TypeError):
            yield from self.redis.zadd(None, 1, b'one')
        with self.assertRaises(TypeError):
            yield from self.redis.zadd(key, b'two', b'one')
        with self.assertRaises(TypeError):
            yield from self.redis.zadd(key, 3, b'tree', 4)
        with self.assertRaises(TypeError):
            yield from self.redis.zadd(key, 3, b'tree', 'four', 4)

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
