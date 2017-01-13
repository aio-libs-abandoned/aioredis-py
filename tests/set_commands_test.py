import asyncio
import pytest


@asyncio.coroutine
def add(redis, key, members):
    ok = yield from redis.connection.execute(b'sadd', key, members)
    assert ok == 1


@pytest.mark.run_loop
def test_sadd(redis):
    key, member = b'key:sadd', b'hello'
    # add member to the set, expected result: 1
    test_result = yield from redis.sadd(key, member)
    assert test_result == 1

    # add other value, expected result: 1
    test_result = yield from redis.sadd(key, b'world')
    assert test_result == 1

    # add existing member to the set, expected result: 0
    test_result = yield from redis.sadd(key, member)
    assert test_result == 0

    with pytest.raises(TypeError):
        yield from redis.sadd(None, 10)


@pytest.mark.run_loop
def test_scard(redis):
    key, member = b'key:scard', b'hello'

    # check that our set is empty one
    empty_size = yield from redis.scard(key)
    assert empty_size == 0

    # add more members to the set and check, set size on every step
    for i in range(1, 11):
        incr = str(i).encode('utf-8')
        yield from add(redis, key, member + incr)
        current_size = yield from redis.scard(key)
        assert current_size == i

    with pytest.raises(TypeError):
        yield from redis.scard(None)


@pytest.mark.run_loop
def test_sdiff(redis):
    key1 = b'key:sdiff:1'
    key2 = b'key:sdiff:2'
    key3 = b'key:sdiff:3'

    members1 = (b'a', b'b', b'c', b'd')
    members2 = (b'c',)
    members3 = (b'a', b'c', b'e')

    yield from redis.sadd(key1, *members1)
    yield from redis.sadd(key2, *members2)
    yield from redis.sadd(key3, *members3)

    # test multiple keys
    test_result = yield from redis.sdiff(key1, key2, key3)
    assert set(test_result) == {b'b', b'd'}

    # test single key
    test_result = yield from redis.sdiff(key2)
    assert set(test_result) == {b'c'}

    with pytest.raises(TypeError):
        yield from redis.sdiff(None)
    with pytest.raises(TypeError):
        yield from redis.sdiff(key1, None)


@pytest.mark.run_loop
def test_sdiffstore(redis):
    key1 = b'key:sdiffstore:1'
    key2 = b'key:sdiffstore:2'
    destkey = b'key:sdiffstore:destkey'
    members1 = (b'a', b'b', b'c')
    members2 = (b'c', b'd', b'e')

    yield from redis.sadd(key1, *members1)
    yield from redis.sadd(key2, *members2)

    # test basic use case, expected: since diff contains only two members
    test_result = yield from redis.sdiffstore(destkey, key1, key2)
    assert test_result == 2

    # make sure that destkey contains 2 members
    test_result = yield from redis.scard(destkey)
    assert test_result == 2

    # try sdiffstore in case none of sets exists
    test_result = yield from redis.sdiffstore(
        b'not:' + destkey, b'not:' + key1, b'not:' + key2)
    assert test_result == 0

    with pytest.raises(TypeError):
        yield from redis.sdiffstore(None, key1)
    with pytest.raises(TypeError):
        yield from redis.sdiffstore(destkey, None)
    with pytest.raises(TypeError):
        yield from redis.sdiffstore(destkey, key1, None)


@pytest.mark.run_loop
def test_sinter(redis):
    key1 = b'key:sinter:1'
    key2 = b'key:sinter:2'
    key3 = b'key:sinter:3'

    members1 = (b'a', b'b', b'c', b'd')
    members2 = (b'c',)
    members3 = (b'a', b'c', b'e')

    yield from redis.sadd(key1, *members1)
    yield from redis.sadd(key2, *members2)
    yield from redis.sadd(key3, *members3)

    # test multiple keys
    test_result = yield from redis.sinter(key1, key2, key3)
    assert set(test_result) == {b'c'}

    # test single key
    test_result = yield from redis.sinter(key2)
    assert set(test_result) == {b'c'}

    with pytest.raises(TypeError):
        yield from redis.sinter(None)
    with pytest.raises(TypeError):
        yield from redis.sinter(key1, None)


@pytest.mark.run_loop
def test_sinterstore(redis):
    key1 = b'key:sinterstore:1'
    key2 = b'key:sinterstore:2'
    destkey = b'key:sinterstore:destkey'
    members1 = (b'a', b'b', b'c')
    members2 = (b'c', b'd', b'e')

    yield from redis.sadd(key1, *members1)
    yield from redis.sadd(key2, *members2)

    # test basic use case, expected: since inter contains only one member
    test_result = yield from redis.sinterstore(destkey, key1, key2)
    assert test_result == 1

    # make sure that destkey contains only one member
    test_result = yield from redis.scard(destkey)
    assert test_result == 1

    # try sinterstore in case none of sets exists
    test_result = yield from redis.sinterstore(
        b'not:' + destkey, b'not:' + key1, b'not:' + key2)
    assert test_result == 0

    with pytest.raises(TypeError):
        yield from redis.sinterstore(None, key1)
    with pytest.raises(TypeError):
        yield from redis.sinterstore(destkey, None)
    with pytest.raises(TypeError):
        yield from redis.sinterstore(destkey, key1, None)


@pytest.mark.run_loop
def test_sismember(redis):
    key, member = b'key:sismember', b'hello'
    # add member to the set, expected result: 1
    test_result = yield from redis.sadd(key, member)
    assert test_result == 1

    # test that value in set
    test_result = yield from redis.sismember(key, member)
    assert test_result == 1
    # test that value not in set
    test_result = yield from redis.sismember(key, b'world')
    assert test_result == 0

    with pytest.raises(TypeError):
        yield from redis.sismember(None, b'world')


@pytest.mark.run_loop
def test_smembers(redis):
    key = b'key:smembers'
    member1 = b'hello'
    member2 = b'world'

    yield from redis.sadd(key, member1)
    yield from redis.sadd(key, member2)

    # test not empty set
    test_result = yield from redis.smembers(key)
    assert set(test_result) == {member1, member2}

    # test empty set
    test_result = yield from redis.smembers(b'not:' + key)
    assert test_result == []

    # test encoding param
    test_result = yield from redis.smembers(key, encoding='utf-8')
    assert set(test_result) == {'hello', 'world'}

    with pytest.raises(TypeError):
        yield from redis.smembers(None)


@pytest.mark.run_loop
def test_smove(redis):
    key1 = b'key:smove:1'
    key2 = b'key:smove:2'
    member1 = b'one'
    member2 = b'two'
    member3 = b'three'
    yield from redis.sadd(key1, member1, member2)
    yield from redis.sadd(key2, member3)
    # move member2 to second set
    test_result = yield from redis.smove(key1, key2, member2)
    assert test_result == 1
    # check first set, member should be removed
    test_result = yield from redis.smembers(key1)
    assert test_result == [member1]
    # check second set, member should be added
    test_result = yield from redis.smembers(key2)
    assert set(test_result) == {member2, member3}

    # move to empty set
    test_result = yield from redis.smove(
        key1, b'not:' + key2, member1)
    assert test_result == 1

    # move from empty set (set with under key1 is empty now
    test_result = yield from redis.smove(
        key1, b'not:' + key2, member1)
    assert test_result == 0

    # move from set that does not exists to set tha does not exists too
    test_result = yield from redis.smove(
        b'not:' + key1, b'other:not:' + key2, member1)
    assert test_result == 0

    with pytest.raises(TypeError):
        yield from redis.smove(None, key1, member1)
    with pytest.raises(TypeError):
        yield from redis.smove(key1, None, member1)


@pytest.mark.run_loop
def test_spop(redis):
    key = b'key:spop:1'
    members = b'one', b'two', b'three'
    yield from redis.sadd(key, *members)

    for _ in members:
        test_result = yield from redis.spop(key)
        assert test_result in members

    # test with encoding
    members = 'four', 'five', 'six'
    yield from redis.sadd(key, *members)

    for _ in members:
        test_result = yield from redis.spop(key, encoding='utf-8')
        assert test_result in members

    # make sure set is empty, after all values poped
    test_result = yield from redis.smembers(key)
    assert test_result == []

    # try to pop data from empty set
    test_result = yield from redis.spop(b'not:' + key)
    assert test_result is None

    with pytest.raises(TypeError):
        yield from redis.spop(None)


@pytest.mark.run_loop
def test_srandmember(redis):
    key = b'key:srandmember:1'
    members = b'one', b'two', b'three', b'four', b'five', b'six', b'seven'
    yield from redis.sadd(key, *members)

    for _ in members:
        test_result = yield from redis.srandmember(key)
        assert test_result in members

    # test with encoding
    test_result = yield from redis.srandmember(key, encoding='utf-8')
    strings = {'one', 'two', 'three', 'four', 'five', 'six', 'seven'}
    assert test_result in strings

    # make sure set contains all values, and nothing missing
    test_result = yield from redis.smembers(key)
    assert set(test_result) == set(members)

    # fetch 4 elements for the first time, as result 4 distinct values
    test_result1 = yield from redis.srandmember(key, 4)
    assert len(test_result1) == 4
    assert set(test_result1).issubset(members) is True

    # test negative count, same element may be returned multiple times
    test_result2 = yield from redis.srandmember(key, -10)
    assert len(test_result2) == 10
    assert set(test_result2).issubset(members) is True
    assert len(set(test_result2)) <= len(members)

    # pull member from empty set
    test_result = yield from redis.srandmember(b'not' + key)
    assert test_result is None

    with pytest.raises(TypeError):
        yield from redis.srandmember(None)


@pytest.mark.run_loop
def test_srem(redis):
    key = b'key:srem:1'
    members = b'one', b'two', b'three', b'four', b'five', b'six', b'seven'
    yield from redis.sadd(key, *members)

    # remove one element from set
    test_result = yield from redis.srem(key, members[-1])
    assert test_result == 1

    # remove not existing element
    test_result = yield from redis.srem(key, b'foo')
    assert test_result == 0

    # remove not existing element from not existing set
    test_result = yield from redis.srem(b'not:' + key, b'foo')
    assert test_result == 0

    # remove multiple elements from set
    test_result = yield from redis.srem(key, *members[:-1])
    assert test_result == 6
    with pytest.raises(TypeError):
        yield from redis.srem(None, members)


@pytest.mark.run_loop
def test_sunion(redis):
    key1 = b'key:sunion:1'
    key2 = b'key:sunion:2'
    key3 = b'key:sunion:3'

    members1 = [b'a', b'b', b'c', b'd']
    members2 = [b'c']
    members3 = [b'a', b'c', b'e']

    yield from redis.sadd(key1, *members1)
    yield from redis.sadd(key2, *members2)
    yield from redis.sadd(key3, *members3)

    # test multiple keys
    test_result = yield from redis.sunion(key1, key2, key3)
    assert set(test_result) == set(members1 + members2 + members3)

    # test single key
    test_result = yield from redis.sunion(key2)
    assert set(test_result) == {b'c'}

    with pytest.raises(TypeError):
        yield from redis.sunion(None)
    with pytest.raises(TypeError):
        yield from redis.sunion(key1, None)


@pytest.mark.run_loop
def test_sunionstore(redis):
    key1 = b'key:sunionstore:1'
    key2 = b'key:sunionstore:2'
    destkey = b'key:sunionstore:destkey'
    members1 = (b'a', b'b', b'c')
    members2 = (b'c', b'd', b'e')

    yield from redis.sadd(key1, *members1)
    yield from redis.sadd(key2, *members2)

    # test basic use case
    test_result = yield from redis.sunionstore(destkey, key1, key2)
    assert test_result == 5

    # make sure that destkey contains 5 members
    test_result = yield from redis.scard(destkey)
    assert test_result == 5

    # try sunionstore in case none of sets exists
    test_result = yield from redis.sunionstore(
        b'not:' + destkey, b'not:' + key1, b'not:' + key2)
    assert test_result == 0

    with pytest.raises(TypeError):
        yield from redis.sunionstore(None, key1)
    with pytest.raises(TypeError):
        yield from redis.sunionstore(destkey, None)
    with pytest.raises(TypeError):
        yield from redis.sunionstore(destkey, key1, None)


@pytest.redis_version(2, 8, 0, reason='SSCAN is available since redis>=2.8.0')
@pytest.mark.run_loop
def test_sscan(redis):
    key = b'key:sscan'
    for i in range(1, 11):
        foo_or_bar = 'bar' if i % 3 else 'foo'
        member = 'member:{}:{}'.format(foo_or_bar, i).encode('utf-8')
        yield from add(redis, key, member)

    cursor, values = yield from redis.sscan(
        key, match=b'member:foo:*')
    assert len(values) == 3

    cursor, values = yield from redis.sscan(
        key, match=b'member:bar:*')
    assert len(values) == 7

    # SCAN family functions do not guarantee that the number (count) of
    # elements returned per call are in a given range. So here
    # just dummy test, that *count* argument does not break something
    cursor = b'0'
    test_values = []
    while cursor:
        cursor, values = yield from redis.sscan(key, cursor, count=2)
        test_values.extend(values)
    assert len(test_values) == 10

    with pytest.raises(TypeError):
        yield from redis.sscan(None)
