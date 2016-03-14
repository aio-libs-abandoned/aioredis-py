import unittest

from ._testutil import RedisTest, run_until_complete, REDIS_VERSION, IS_REDIS_CLUSTER


@unittest.skipIf(REDIS_VERSION < (2, 8, 9),
                 'HyperLogLog works only with redis>=2.8.9')
class HyperLogLogCommandsTest(RedisTest):

    @run_until_complete
    def test_pfcount(self):
        key = '{key:pfcount}:1'
        other_key = '{key:pfcount}:2'

        # add initial data, cardinality changed so command returns 1
        is_changed = yield from self.redis.pfadd(key, 'foo', 'bar', 'zap')
        self.assertEqual(is_changed, 1)

        # add more data, cardinality not changed so command returns 0
        is_changed = yield from self.redis.pfadd(key, 'zap', 'zap', 'zap')
        self.assertEqual(is_changed, 0)

        # add event more data, cardinality not changed so command returns 0
        is_changed = yield from self.redis.pfadd(key, 'foo', 'bar')
        self.assertEqual(is_changed, 0)

        # check cardinality of one key
        cardinality = yield from self.redis.pfcount(key)
        self.assertEqual(cardinality, 3)

        # create new key (variable) for cardinality estimation
        is_changed = yield from self.redis.pfadd(other_key, 1, 2, 3)
        self.assertEqual(is_changed, 1)

        # check cardinality of multiple keys
        cardinality = yield from self.redis.pfcount(key, other_key)
        self.assertEqual(cardinality, 6)

        with self.assertRaises(TypeError):
            yield from self.redis.pfcount(None)
        with self.assertRaises(TypeError):
            yield from self.redis.pfcount(key, None)
        with self.assertRaises(TypeError):
            yield from self.redis.pfcount(key, key, None)

    @run_until_complete
    def test_pfadd(self):
        key = 'hll_pfadd'
        values = ['a', 's', 'y', 'n', 'c', 'i', 'o']
        # add initial data, cardinality changed so command returns 1
        is_changed = yield from self.redis.pfadd(key, *values)
        self.assertEqual(is_changed, 1)
        # add event more data, cardinality not changed so command returns 0
        is_changed = yield from self.redis.pfadd(key, 'i', 'o')
        self.assertEqual(is_changed, 0)

    @run_until_complete
    def test_pfadd_wrong_input(self):
        with self.assertRaises(TypeError):
            yield from self.redis.pfadd(None, 'value')

    @run_until_complete
    def test_pfmerge(self):
        key = '{key:pfmerge}:1'
        key_other = '{key:pfmerge}:2'

        key_dest = '{key:pfmerge}:dest'

        values = ['a', 's', 'y', 'n', 'c', 'i', 'o']
        values_other = ['a', 'i', 'o', 'r', 'e', 'd', 'i', 's']

        data_set = set(values + values_other)
        cardinality_merged = len(data_set)

        # add initial data, cardinality changed so command returns 1
        yield from self.redis.pfadd(key, *values)
        yield from self.redis.pfadd(key_other, *values_other)

        # check cardinality of one key
        cardinality = yield from self.redis.pfcount(key)
        self.assertEqual(cardinality, len(set(values_other)))

        cardinality_other = yield from self.redis.pfcount(key_other)
        self.assertEqual(cardinality_other, len(set(values_other)))

        yield from self.redis.pfmerge(key_dest, key, key_other)
        cardinality_dest = yield from self.redis.pfcount(key_dest)
        self.assertEqual(cardinality_dest, cardinality_merged)

        with self.assertRaises(TypeError):
            yield from self.redis.pfmerge(None, key)
        with self.assertRaises(TypeError):
            yield from self.redis.pfmerge(key_dest, None)
        with self.assertRaises(TypeError):
            yield from self.redis.pfmerge(key_dest, key, None)

    @run_until_complete
    def test_pfmerge_wrong_input(self):
        with self.assertRaises(TypeError):
            yield from self.redis.pfmerge(None, 'value')
