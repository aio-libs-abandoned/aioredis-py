import asyncio
import unittest

from ._testutil import RedisTest, run_until_complete, REDIS_VERSION


class SetCommandsTest(RedisTest):

    @asyncio.coroutine
    def add(self, key, memeber):
        ok = yield from self.redis.connection.execute(b'sadd', key, memeber)
        self.assertEqual(ok, 1)

    @run_until_complete
    def test_sadd(self):
        key, member = b'key:sadd', b'hello'
        # add member to the set, expected result: 1
        test_result = yield from self.redis.sadd(key, member)
        self.assertEqual(test_result, 1)

        # add other value, expected result: 1
        test_result = yield from self.redis.sadd(key, b'world')
        self.assertEqual(test_result, 1)

        # add existing member to the set, expected result: 0
        test_result = yield from self.redis.sadd(key, member)
        self.assertEqual(test_result, 0)

        with self.assertRaises(TypeError):
            yield from self.redis.sadd(None, 10)

    @run_until_complete
    def test_scard(self):
        key, member = b'key:scard', b'hello'

        # check that our set is empty one
        empty_size = yield from self.redis.scard(key)
        self.assertEqual(empty_size, 0)

        # add more members to the set and check, set size on every step
        for i in range(1, 11):
            incr = str(i).encode('utf-8')
            yield from self.add(key, member + incr)
            current_size = yield from self.redis.scard(key)
            self.assertEqual(current_size, i)

        with self.assertRaises(TypeError):
            yield from self.redis.scard(None)

    @run_until_complete
    def test_sdiff(self):
        key1 = b'key:sdiff:1'
        key2 = b'key:sdiff:2'
        key3 = b'key:sdiff:3'

        members1 = (b'a', b'b', b'c', b'd')
        members2 = (b'c',)
        members3 = (b'a', b'c', b'e')

        yield from self.redis.sadd(key1, *members1)
        yield from self.redis.sadd(key2, *members2)
        yield from self.redis.sadd(key3, *members3)

        # test multiple keys
        test_result = yield from self.redis.sdiff(key1, key2, key3)
        self.assertEqual(set(test_result), {b'b', b'd'})

        # test single key
        test_result = yield from self.redis.sdiff(key2)
        self.assertEqual(set(test_result), {b'c'})

        with self.assertRaises(TypeError):
            yield from self.redis.sdiff(None)
        with self.assertRaises(TypeError):
            yield from self.redis.sdiff(key1, None)

    @run_until_complete
    def test_sdiffstore(self):
        key1 = b'key:sdiffstore:1'
        key2 = b'key:sdiffstore:2'
        destkey = b'key:sdiffstore:destkey'
        members1 = (b'a', b'b', b'c')
        members2 = (b'c', b'd', b'e')

        yield from self.redis.sadd(key1, *members1)
        yield from self.redis.sadd(key2, *members2)

        # test basic use case, expected: since diff contains only two members
        test_result = yield from self.redis.sdiffstore(destkey, key1, key2)
        self.assertEqual(test_result, 2)

        # make sure that destkey contains 2 members
        test_result = yield from self.redis.scard(destkey)
        self.assertEqual(test_result, 2)

        # try sdiffstore in case none of sets exists
        test_result = yield from self.redis.sdiffstore(
            b'not:' + destkey, b'not:' + key1, b'not:' + key2)
        self.assertEqual(test_result, 0)

        with self.assertRaises(TypeError):
            yield from self.redis.sdiffstore(None, key1)
        with self.assertRaises(TypeError):
            yield from self.redis.sdiffstore(destkey, None)
        with self.assertRaises(TypeError):
            yield from self.redis.sdiffstore(destkey, key1, None)

    @run_until_complete
    def test_sinter(self):
        key1 = b'key:sinter:1'
        key2 = b'key:sinter:2'
        key3 = b'key:sinter:3'

        members1 = (b'a', b'b', b'c', b'd')
        members2 = (b'c',)
        members3 = (b'a', b'c', b'e')

        yield from self.redis.sadd(key1, *members1)
        yield from self.redis.sadd(key2, *members2)
        yield from self.redis.sadd(key3, *members3)

        # test multiple keys
        test_result = yield from self.redis.sinter(key1, key2, key3)
        self.assertEqual(set(test_result), {b'c'})

        # test single key
        test_result = yield from self.redis.sinter(key2)
        self.assertEqual(set(test_result), {b'c'})

        with self.assertRaises(TypeError):
            yield from self.redis.sinter(None)
        with self.assertRaises(TypeError):
            yield from self.redis.sinter(key1, None)

    @run_until_complete
    def test_sinterstore(self):
        key1 = b'key:sinterstore:1'
        key2 = b'key:sinterstore:2'
        destkey = b'key:sinterstore:destkey'
        members1 = (b'a', b'b', b'c')
        members2 = (b'c', b'd', b'e')

        yield from self.redis.sadd(key1, *members1)
        yield from self.redis.sadd(key2, *members2)

        # test basic use case, expected: since inter contains only one member
        test_result = yield from self.redis.sinterstore(destkey, key1, key2)
        self.assertEqual(test_result, 1)

        # make sure that destkey contains only one member
        test_result = yield from self.redis.scard(destkey)
        self.assertEqual(test_result, 1)

        # try sinterstore in case none of sets exists
        test_result = yield from self.redis.sinterstore(
            b'not:' + destkey, b'not:' + key1, b'not:' + key2)
        self.assertEqual(test_result, 0)

        with self.assertRaises(TypeError):
            yield from self.redis.sinterstore(None, key1)
        with self.assertRaises(TypeError):
            yield from self.redis.sinterstore(destkey, None)
        with self.assertRaises(TypeError):
            yield from self.redis.sinterstore(destkey, key1, None)

    @run_until_complete
    def test_sismember(self):
        key, member = b'key:sismember', b'hello'
        # add member to the set, expected result: 1
        test_result = yield from self.redis.sadd(key, member)
        self.assertEqual(test_result, 1)

        # test that value in set
        test_result = yield from self.redis.sismember(key, member)
        self.assertEqual(test_result, 1)
        # test that value not in set
        test_result = yield from self.redis.sismember(key, b'world')
        self.assertEqual(test_result, 0)

        with self.assertRaises(TypeError):
            yield from self.redis.sismember(None, b'world')

    @run_until_complete
    def test_smembers(self):
        key = b'key:smembers'
        member1 = b'hello'
        member2 = b'world'

        yield from self.redis.sadd(key, member1)
        yield from self.redis.sadd(key, member2)

        # test not empty set
        test_result = yield from self.redis.smembers(key)
        self.assertEqual(set(test_result), {member1, member2})

        # test empty set
        test_result = yield from self.redis.smembers(b'not:' + key)
        self.assertEqual(test_result, [])

        # test encoding param
        test_result = yield from self.redis.smembers(key, encoding='utf-8')
        self.assertEqual(set(test_result), {'hello', 'world'})

        with self.assertRaises(TypeError):
            yield from self.redis.smembers(None)

    @run_until_complete
    def test_smove(self):
        key1 = b'key:smove:1'
        key2 = b'key:smove:2'
        member1 = b'one'
        member2 = b'two'
        member3 = b'three'
        yield from self.redis.sadd(key1, member1, member2)
        yield from self.redis.sadd(key2, member3)
        # move member2 to second set
        test_result = yield from self.redis.smove(key1, key2, member2)
        self.assertEqual(test_result, 1)
        # check first set, member should be removed
        test_result = yield from self.redis.smembers(key1)
        self.assertEqual(test_result, [member1])
        # check second set, member should be added
        test_result = yield from self.redis.smembers(key2)
        self.assertSetEqual(set(test_result), {member2, member3})

        # move to empty set
        test_result = yield from self.redis.smove(
            key1, b'not:' + key2, member1)
        self.assertEqual(test_result, 1)

        # move from empty set (set with under key1 is empty now
        test_result = yield from self.redis.smove(
            key1, b'not:' + key2, member1)
        self.assertEqual(test_result, 0)

        # move from set that does not exists to set tha does not exists too
        test_result = yield from self.redis.smove(
            b'not:' + key1, b'other:not:' + key2, member1)
        self.assertEqual(test_result, 0)

        with self.assertRaises(TypeError):
            yield from self.redis.smove(None, key1, member1)
        with self.assertRaises(TypeError):
            yield from self.redis.smove(key1, None, member1)

    @run_until_complete
    def test_spop(self):
        key = b'key:spop:1'
        members = b'one', b'two', b'three'
        yield from self.redis.sadd(key, *members)

        for _ in members:
            test_result = yield from self.redis.spop(key)
            self.assertIn(test_result, members)

        # test with encoding
        members = 'four', 'five', 'six'
        yield from self.redis.sadd(key, *members)

        for _ in members:
            test_result = yield from self.redis.spop(key, encoding='utf-8')
            self.assertIn(test_result, members)

        # make sure set is empty, after all values poped
        test_result = yield from self.redis.smembers(key)
        self.assertEqual(test_result, [])

        # try to pop data from empty set
        test_result = yield from self.redis.spop(b'not:' + key)
        self.assertEqual(test_result, None)

        with self.assertRaises(TypeError):
            yield from self.redis.spop(None)

    @run_until_complete
    def test_srandmember(self):
        key = b'key:srandmember:1'
        members = b'one', b'two', b'three', b'four', b'five', b'six', b'seven'
        yield from self.redis.sadd(key, *members)

        for _ in members:
            test_result = yield from self.redis.srandmember(key)
            self.assertIn(test_result, members)

        # test with encoding
        test_result = yield from self.redis.srandmember(key, encoding='utf-8')
        strings = {'one', 'two', 'three', 'four', 'five', 'six', 'seven'}
        self.assertIn(test_result, strings)

        # make sure set contains all values, and nothing missing
        test_result = yield from self.redis.smembers(key)
        self.assertSetEqual(set(test_result), set(members))

        # fetch 4 elements for the first time, as result 4 distinct values
        test_result1 = yield from self.redis.srandmember(key, 4)
        self.assertEqual(len(test_result1), 4)
        self.assertTrue(set(test_result1).issubset(members))

        # test negative count, same element may be returned multiple times
        test_result2 = yield from self.redis.srandmember(key, -10)
        self.assertEqual(len(test_result2), 10)
        self.assertTrue(set(test_result2).issubset(members))
        self.assertLessEqual(len(set(test_result2)), len(members))

        # pull member from empty set
        test_result = yield from self.redis.srandmember(b'not' + key)
        self.assertEqual(test_result, None)

        with self.assertRaises(TypeError):
            yield from self.redis.srandmember(None)

    @run_until_complete
    def test_srem(self):
        key = b'key:srem:1'
        members = b'one', b'two', b'three', b'four', b'five', b'six', b'seven'
        yield from self.redis.sadd(key, *members)

        # remove one element from set
        test_result = yield from self.redis.srem(key, members[-1])
        self.assertEqual(test_result, 1)

        # remove not existing element
        test_result = yield from self.redis.srem(key, b'foo')
        self.assertEqual(test_result, 0)

        # remove not existing element from not existing set
        test_result = yield from self.redis.srem(b'not:' + key, b'foo')
        self.assertEqual(test_result, 0)

        # remove multiple elements from set
        test_result = yield from self.redis.srem(key, *members[:-1])
        self.assertEqual(test_result, 6)
        with self.assertRaises(TypeError):
            yield from self.redis.srem(None, members)

    @run_until_complete
    def test_sunion(self):
        key1 = b'key:sunion:1'
        key2 = b'key:sunion:2'
        key3 = b'key:sunion:3'

        members1 = [b'a', b'b', b'c', b'd']
        members2 = [b'c']
        members3 = [b'a', b'c', b'e']

        yield from self.redis.sadd(key1, *members1)
        yield from self.redis.sadd(key2, *members2)
        yield from self.redis.sadd(key3, *members3)

        # test multiple keys
        test_result = yield from self.redis.sunion(key1, key2, key3)
        self.assertEqual(set(test_result), set(members1 + members2 + members3))

        # test single key
        test_result = yield from self.redis.sunion(key2)
        self.assertEqual(set(test_result), {b'c'})

        with self.assertRaises(TypeError):
            yield from self.redis.sunion(None)
        with self.assertRaises(TypeError):
            yield from self.redis.sunion(key1, None)

    @run_until_complete
    def test_sunionstore(self):
        key1 = b'key:sunionstore:1'
        key2 = b'key:sunionstore:2'
        destkey = b'key:sunionstore:destkey'
        members1 = (b'a', b'b', b'c')
        members2 = (b'c', b'd', b'e')

        yield from self.redis.sadd(key1, *members1)
        yield from self.redis.sadd(key2, *members2)

        # test basic use case
        test_result = yield from self.redis.sunionstore(destkey, key1, key2)
        self.assertEqual(test_result, 5)

        # make sure that destkey contains 5 members
        test_result = yield from self.redis.scard(destkey)
        self.assertEqual(test_result, 5)

        # try sunionstore in case none of sets exists
        test_result = yield from self.redis.sunionstore(
            b'not:' + destkey, b'not:' + key1, b'not:' + key2)
        self.assertEqual(test_result, 0)

        with self.assertRaises(TypeError):
            yield from self.redis.sunionstore(None, key1)
        with self.assertRaises(TypeError):
            yield from self.redis.sunionstore(destkey, None)
        with self.assertRaises(TypeError):
            yield from self.redis.sunionstore(destkey, key1, None)

    @unittest.skipIf(REDIS_VERSION < (2, 8, 0),
                     'SSCAN is available since redis>=2.8.0')
    @run_until_complete
    def test_sscan(self):
        key = b'key:sscan'
        for i in range(1, 11):
            foo_or_bar = 'bar' if i % 3 else 'foo'
            member = 'member:{}:{}'.format(foo_or_bar, i).encode('utf-8')
            yield from self.add(key, member)

        cursor, values = yield from self.redis.sscan(
            key, match=b'member:foo:*')
        self.assertEqual(len(values), 3)

        cursor, values = yield from self.redis.sscan(
            key, match=b'member:bar:*')
        self.assertEqual(len(values), 7)

        # SCAN family functions do not guarantee that the number (count) of
        # elements returned per call are in a given range. So here
        # just dummy test, that *count* argument does not break something
        cursor = b'0'
        test_values = []
        while cursor:
            cursor, values = yield from self.redis.sscan(key, cursor, count=2)
            test_values.extend(values)
        self.assertEqual(len(test_values), 10)

        with self.assertRaises(TypeError):
            yield from self.redis.sscan(None)
