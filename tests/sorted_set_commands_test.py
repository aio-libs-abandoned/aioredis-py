import itertools
import pytest


@pytest.mark.run_loop
def test_zadd(redis):
    key = b'key:zadd'
    res = yield from redis.zadd(key, 1, b'one')
    assert res == 1
    res = yield from redis.zadd(key, 1, b'one')
    assert res == 0
    res = yield from redis.zadd(key, 1, b'uno')
    assert res == 1
    res = yield from redis.zadd(key, 2.5, b'two')
    assert res == 1

    res = yield from redis.zadd(key, 3, b'three', 4, b'four')
    assert res == 2
    res = yield from redis.zrange(key, 0, -1, withscores=False)
    assert res == [b'one', b'uno', b'two', b'three', b'four']

    with pytest.raises(TypeError):
        yield from redis.zadd(None, 1, b'one')
    with pytest.raises(TypeError):
        yield from redis.zadd(key, b'two', b'one')
    with pytest.raises(TypeError):
        yield from redis.zadd(key, 3, b'three', 4)
    with pytest.raises(TypeError):
        yield from redis.zadd(key, 3, b'three', 'four', 4)


@pytest.mark.run_loop
def test_zcard(redis):
    key = b'key:zcard'
    pairs = [1, b'one', 2, b'two', 3, b'three']
    res = yield from redis.zadd(key, *pairs)
    assert res == 3
    res = yield from redis.zcard(key)
    assert res == 3
    res = yield from redis.zadd(key, 1, b'ein')
    assert res == 1
    res = yield from redis.zcard(key)
    assert res == 4

    with pytest.raises(TypeError):
        yield from redis.zcard(None)


@pytest.mark.run_loop
def test_zcount(redis):
    key = b'key:zcount'
    pairs = [1, b'one', 1, b'uno', 2.5, b'two', 3, b'three', 7, b'seven']
    res = yield from redis.zadd(key, *pairs)
    assert res == 5

    res_zcount = yield from redis.zcount(key)
    res_zcard = yield from redis.zcard(key)
    assert res_zcount == res_zcard

    res = yield from redis.zcount(key, 1, 3)
    assert res == 4
    res = yield from redis.zcount(key, 3, 10)
    assert res == 2
    res = yield from redis.zcount(key, 100, 200)
    assert res == 0

    res = yield from redis.zcount(
        key, 1, 3, exclude=redis.ZSET_EXCLUDE_BOTH)
    assert res == 1
    res = yield from redis.zcount(
        key, 1, 3, exclude=redis.ZSET_EXCLUDE_MIN)
    assert res == 2
    res = yield from redis.zcount(
        key, 1, 3, exclude=redis.ZSET_EXCLUDE_MAX)
    assert res == 3
    res = yield from redis.zcount(
        key, 1, exclude=redis.ZSET_EXCLUDE_MAX)
    assert res == 5
    res = yield from redis.zcount(
        key, float('-inf'), 3, exclude=redis.ZSET_EXCLUDE_MIN)
    assert res == 4

    with pytest.raises(TypeError):
        yield from redis.zcount(None)
    with pytest.raises(TypeError):
        yield from redis.zcount(key, 'one', 2)
    with pytest.raises(TypeError):
        yield from redis.zcount(key, 1.1, b'two')
    with pytest.raises(ValueError):
        yield from redis.zcount(key, 10, 1)


@pytest.mark.run_loop
def test_zincrby(redis):
    key = b'key:zincrby'
    pairs = [1, b'one', 1, b'uno', 2.5, b'two', 3, b'three']
    res = yield from redis.zadd(key, *pairs)
    res = yield from redis.zincrby(key, 1, b'one')
    assert res == 2
    res = yield from redis.zincrby(key, -5, b'uno')
    assert res == -4
    res = yield from redis.zincrby(key, 3.14, b'two')
    assert abs(res - 5.64) <= 0.00001
    res = yield from redis.zincrby(key, -3.14, b'three')
    assert abs(res - -0.14) <= 0.00001

    with pytest.raises(TypeError):
        yield from redis.zincrby(None, 5, 'one')
    with pytest.raises(TypeError):
        yield from redis.zincrby(key, 'one', 5)


@pytest.mark.run_loop
def test_zinterstore(redis):
    zset1 = [2, 'one', 2, 'two']
    zset2 = [3, 'one', 3, 'three']

    yield from redis.zadd('zset1', *zset1)
    yield from redis.zadd('zset2', *zset2)

    res = yield from redis.zinterstore('zout', 'zset1', 'zset2')
    assert res == 1
    res = yield from redis.zrange('zout', withscores=True)
    assert res == [b'one', 5]

    res = yield from redis.zinterstore(
        'zout', 'zset1', 'zset2',
        aggregate=redis.ZSET_AGGREGATE_SUM)
    assert res == 1
    res = yield from redis.zrange('zout', withscores=True)
    assert res == [b'one', 5]

    res = yield from redis.zinterstore(
        'zout', 'zset1', 'zset2',
        aggregate=redis.ZSET_AGGREGATE_MIN)
    assert res == 1
    res = yield from redis.zrange('zout', withscores=True)
    assert res == [b'one', 2]

    res = yield from redis.zinterstore(
        'zout', 'zset1', 'zset2',
        aggregate=redis.ZSET_AGGREGATE_MAX)
    assert res == 1
    res = yield from redis.zrange('zout', withscores=True)
    assert res == [b'one', 3]

    # weights

    with pytest.raises(AssertionError):
        yield from redis.zinterstore('zout', 'zset1', 'zset2',
                                     with_weights=True)

    res = yield from redis.zinterstore('zout',
                                       ('zset1', 2), ('zset2', 2),
                                       with_weights=True)
    assert res == 1
    res = yield from redis.zrange('zout', withscores=True)
    assert res == [b'one', 10]


@pytest.redis_version(
    2, 8, 9, reason='ZLEXCOUNT is available since redis>=2.8.9')
@pytest.mark.run_loop
def test_zlexcount(redis):
    key = b'key:zlexcount'
    pairs = [0, b'a', 0, b'b', 0, b'c', 0, b'd', 0, b'e']
    res = yield from redis.zadd(key, *pairs)
    assert res == 5
    res = yield from redis.zlexcount(key)
    assert res == 5
    res = yield from redis.zlexcount(key, min=b'-', max=b'e')
    assert res == 5
    res = yield from redis.zlexcount(key, min=b'a', max=b'e',
                                     include_min=False,
                                     include_max=False)
    assert res == 3

    with pytest.raises(TypeError):
        yield from redis.zlexcount(None, b'a', b'e')
    with pytest.raises(TypeError):
        yield from redis.zlexcount(key, 10, b'e')
    with pytest.raises(TypeError):
        yield from redis.zlexcount(key, b'a', 20)


@pytest.mark.run_loop
def test_zrange(redis):
    key = b'key:zrange'
    scores = [1, 1, 2.5, 3, 7]
    members = [b'one', b'uno', b'two', b'three', b'seven']
    pairs = list(itertools.chain(*zip(scores, members)))
    rev_pairs = list(itertools.chain(*zip(members, scores)))

    res = yield from redis.zadd(key, *pairs)
    assert res == 5

    res = yield from redis.zrange(key, 0, -1, withscores=False)
    assert res == members
    res = yield from redis.zrange(key, 0, -1, withscores=True)
    assert res == rev_pairs
    res = yield from redis.zrange(key, -2, -1, withscores=False)
    assert res == members[-2:]
    res = yield from redis.zrange(key, 1, 2, withscores=False)
    assert res == members[1:3]
    with pytest.raises(TypeError):
        yield from redis.zrange(None, 1, b'one')
    with pytest.raises(TypeError):
        yield from redis.zrange(key, b'first', -1)
    with pytest.raises(TypeError):
        yield from redis.zrange(key, 0, 'last')


@pytest.redis_version(
    2, 8, 9, reason='ZRANGEBYLEX is available since redis>=2.8.9')
@pytest.mark.run_loop
def test_zrangebylex(redis):
    key = b'key:zrangebylex'
    scores = [0] * 5
    members = [b'a', b'b', b'c', b'd', b'e']
    pairs = list(itertools.chain(*zip(scores, members)))

    res = yield from redis.zadd(key, *pairs)
    assert res == 5
    res = yield from redis.zrangebylex(key)
    assert res == members
    res = yield from redis.zrangebylex(key, min=b'-', max=b'd')
    assert res == members[:-1]
    res = yield from redis.zrangebylex(key, min=b'a', max=b'e',
                                       include_min=False,
                                       include_max=False)
    assert res == members[1:-1]
    res = yield from redis.zrangebylex(key, min=b'x', max=b'z')
    assert res == []
    res = yield from redis.zrangebylex(key, min=b'e', max=b'a')
    assert res == []
    res = yield from redis.zrangebylex(key, offset=1, count=2)
    assert res == members[1:3]
    with pytest.raises(TypeError):
        yield from redis.zrangebylex(None, b'a', b'e')
    with pytest.raises(TypeError):
        yield from redis.zrangebylex(key, 10, b'e')
    with pytest.raises(TypeError):
        yield from redis.zrangebylex(key, b'a', 20)
    with pytest.raises(TypeError):
        yield from redis.zrangebylex(key, b'a', b'e', offset=1)
    with pytest.raises(TypeError):
        yield from redis.zrangebylex(key, b'a', b'e', count=1)
    with pytest.raises(TypeError):
        yield from redis.zrangebylex(key, b'a', b'e',
                                     offset='one', count=1)
    with pytest.raises(TypeError):
        yield from redis.zrangebylex(key, b'a', b'e',
                                     offset=1, count='one')


@pytest.mark.run_loop
def test_zrank(redis):
    key = b'key:zrank'
    scores = [1, 1, 2.5, 3, 7]
    members = [b'one', b'uno', b'two', b'three', b'seven']
    pairs = list(itertools.chain(*zip(scores, members)))

    res = yield from redis.zadd(key, *pairs)
    assert res == 5

    for i, m in enumerate(members):
        res = yield from redis.zrank(key, m)
        assert res == i

    res = yield from redis.zrank(key, b'not:exists')
    assert res is None

    with pytest.raises(TypeError):
        yield from redis.zrank(None, b'one')


@pytest.mark.run_loop
def test_zrangebyscore(redis):
    key = b'key:zrangebyscore'
    scores = [1, 1, 2.5, 3, 7]
    members = [b'one', b'uno', b'two', b'three', b'seven']
    pairs = list(itertools.chain(*zip(scores, members)))
    rev_pairs = list(itertools.chain(*zip(members, scores)))
    res = yield from redis.zadd(key, *pairs)
    assert res == 5
    res = yield from redis.zrangebyscore(key, 1, 7, withscores=False)
    assert res == members
    res = yield from redis.zrangebyscore(
        key, 1, 7, withscores=False, exclude=redis.ZSET_EXCLUDE_BOTH)
    assert res == members[2:-1]
    res = yield from redis.zrangebyscore(key, 1, 7, withscores=True)
    assert res == rev_pairs

    res = yield from redis.zrangebyscore(key, 1, 10, offset=2, count=2)
    assert res == members[2:4]

    with pytest.raises(TypeError):
        yield from redis.zrangebyscore(None, 1, 7)
    with pytest.raises(TypeError):
        yield from redis.zrangebyscore(key, 10, b'e')
    with pytest.raises(TypeError):
        yield from redis.zrangebyscore(key, b'a', 20)
    with pytest.raises(TypeError):
        yield from redis.zrangebyscore(key, 1, 7, offset=1)
    with pytest.raises(TypeError):
        yield from redis.zrangebyscore(key, 1, 7, count=1)
    with pytest.raises(TypeError):
        yield from redis.zrangebyscore(key, 1, 7, offset='one', count=1)
    with pytest.raises(TypeError):
        yield from redis.zrangebyscore(key, 1, 7, offset=1, count='one')


@pytest.mark.run_loop
def test_zrem(redis):
    key = b'key:zrem'
    scores = [1, 1, 2.5, 3, 7]
    members = [b'one', b'uno', b'two', b'three', b'seven']
    pairs = list(itertools.chain(*zip(scores, members)))

    res = yield from redis.zadd(key, *pairs)
    assert res == 5

    res = yield from redis.zrem(key, b'uno', b'one')
    assert res == 2

    res = yield from redis.zrange(key, 0, -1)
    assert res == members[2:]

    res = yield from redis.zrem(key, b'not:exists')
    assert res == 0

    res = yield from redis.zrem(b'not:' + key, b'not:exists')
    assert res == 0

    with pytest.raises(TypeError):
        yield from redis.zrem(None, b'one')


@pytest.redis_version(
    2, 8, 9, reason='ZREMRANGEBYLEX is available since redis>=2.8.9')
@pytest.mark.run_loop
def test_zremrangebylex(redis):
    key = b'key:zremrangebylex'
    members = [b'aaaa', b'b', b'c', b'd', b'e', b'foo', b'zap', b'zip',
               b'ALPHA', b'alpha']
    scores = [0] * len(members)

    pairs = list(itertools.chain(*zip(scores, members)))
    res = yield from redis.zadd(key, *pairs)
    assert res == 10

    res = yield from redis.zremrangebylex(key, b'alpha', b'omega',
                                          include_max=True,
                                          include_min=True)
    assert res == 6
    res = yield from redis.zrange(key, 0, -1)
    assert res == [b'ALPHA', b'aaaa', b'zap', b'zip']

    res = yield from redis.zremrangebylex(key, b'zap', b'zip',
                                          include_max=False,
                                          include_min=False)
    assert res == 0

    res = yield from redis.zrange(key, 0, -1)
    assert res == [b'ALPHA', b'aaaa', b'zap', b'zip']

    res = yield from redis.zremrangebylex(key)
    assert res == 4
    res = yield from redis.zrange(key, 0, -1)
    assert res == []

    with pytest.raises(TypeError):
        yield from redis.zremrangebylex(None, b'a', b'e')
    with pytest.raises(TypeError):
        yield from redis.zremrangebylex(key, 10, b'e')
    with pytest.raises(TypeError):
        yield from redis.zremrangebylex(key, b'a', 20)


@pytest.mark.run_loop
def test_zremrangebyrank(redis):
    key = b'key:zremrangebyrank'
    scores = [0, 1, 2, 3, 4, 5]
    members = [b'zero', b'one', b'two', b'three', b'four', b'five']
    pairs = list(itertools.chain(*zip(scores, members)))
    res = yield from redis.zadd(key, *pairs)
    assert res == 6

    res = yield from redis.zremrangebyrank(key, 0, 1)
    assert res == 2
    res = yield from redis.zrange(key, 0, -1)
    assert res == members[2:]

    res = yield from redis.zremrangebyrank(key, -2, -1)
    assert res == 2
    res = yield from redis.zrange(key, 0, -1)
    assert res == members[2:-2]

    with pytest.raises(TypeError):
        yield from redis.zremrangebyrank(None, 1, 2)
    with pytest.raises(TypeError):
        yield from redis.zremrangebyrank(key, b'first', -1)
    with pytest.raises(TypeError):
        yield from redis.zremrangebyrank(key, 0, 'last')


@pytest.mark.run_loop
def test_zremrangebyscore(redis):
    key = b'key:zremrangebyscore'
    scores = [1, 1, 2.5, 3, 7]
    members = [b'one', b'uno', b'two', b'three', b'seven']
    pairs = list(itertools.chain(*zip(scores, members)))
    res = yield from redis.zadd(key, *pairs)
    assert res == 5

    res = yield from redis.zremrangebyscore(
        key, 3, 7.5, exclude=redis.ZSET_EXCLUDE_MIN)
    assert res == 1
    res = yield from redis.zrange(key, 0, -1)
    assert res == members[:-1]

    res = yield from redis.zremrangebyscore(
        key, 1, 3, exclude=redis.ZSET_EXCLUDE_BOTH)
    assert res == 1
    res = yield from redis.zrange(key, 0, -1)
    assert res == [b'one', b'uno', b'three']

    res = yield from redis.zremrangebyscore(key)
    assert res == 3
    res = yield from redis.zrange(key, 0, -1)
    assert res == []

    with pytest.raises(TypeError):
        yield from redis.zremrangebyscore(None, 1, 2)
    with pytest.raises(TypeError):
        yield from redis.zremrangebyscore(key, b'first', -1)
    with pytest.raises(TypeError):
        yield from redis.zremrangebyscore(key, 0, 'last')


@pytest.mark.run_loop
def test_zrevrange(redis):
    key = b'key:zrevrange'
    scores = [1, 1, 2.5, 3, 7]
    members = [b'one', b'uno', b'two', b'three', b'seven']
    pairs = list(itertools.chain(*zip(scores, members)))

    res = yield from redis.zadd(key, *pairs)
    assert res == 5

    res = yield from redis.zrevrange(key, 0, -1, withscores=False)
    assert res == members[::-1]
    res = yield from redis.zrevrange(key, 0, -1, withscores=True)
    assert res == pairs[::-1]
    res = yield from redis.zrevrange(key, -2, -1, withscores=False)
    assert res == members[1::-1]
    res = yield from redis.zrevrange(key, 1, 2, withscores=False)

    assert res == members[3:1:-1]
    with pytest.raises(TypeError):
        yield from redis.zrevrange(None, 1, b'one')
    with pytest.raises(TypeError):
        yield from redis.zrevrange(key, b'first', -1)
    with pytest.raises(TypeError):
        yield from redis.zrevrange(key, 0, 'last')


@pytest.mark.run_loop
def test_zrevrank(redis):
    key = b'key:zrevrank'
    scores = [1, 1, 2.5, 3, 7]
    members = [b'one', b'uno', b'two', b'three', b'seven']
    pairs = list(itertools.chain(*zip(scores, members)))

    res = yield from redis.zadd(key, *pairs)
    assert res == 5

    for i, m in enumerate(members):
        res = yield from redis.zrevrank(key, m)
        assert res == len(members) - i - 1

    res = yield from redis.zrevrank(key, b'not:exists')
    assert res is None

    with pytest.raises(TypeError):
        yield from redis.zrevrank(None, b'one')


@pytest.mark.run_loop
def test_zscore(redis):
    key = b'key:zscore'
    scores = [1, 1, 2.5, 3, 7]
    members = [b'one', b'uno', b'two', b'three', b'seven']
    pairs = list(itertools.chain(*zip(scores, members)))

    res = yield from redis.zadd(key, *pairs)
    assert res == 5

    for s, m in zip(scores, members):
        res = yield from redis.zscore(key, m)
        assert res == s
    with pytest.raises(TypeError):
        yield from redis.zscore(None, b'one')
    # Check None on undefined members
    res = yield from redis.zscore(key, "undefined")
    assert res is None


@pytest.mark.run_loop
def test_zunionstore(redis):
    zset1 = [2, 'one', 2, 'two']
    zset2 = [3, 'one', 3, 'three']

    yield from redis.zadd('zset1', *zset1)
    yield from redis.zadd('zset2', *zset2)

    res = yield from redis.zunionstore('zout', 'zset1', 'zset2')
    assert res == 3
    res = yield from redis.zrange('zout', withscores=True)
    assert res == [b'two', 2, b'three', 3, b'one', 5]

    res = yield from redis.zunionstore(
        'zout', 'zset1', 'zset2',
        aggregate=redis.ZSET_AGGREGATE_SUM)
    assert res == 3
    res = yield from redis.zrange('zout', withscores=True)
    assert res == [b'two', 2, b'three', 3, b'one', 5]

    res = yield from redis.zunionstore(
        'zout', 'zset1', 'zset2',
        aggregate=redis.ZSET_AGGREGATE_MIN)
    assert res == 3
    res = yield from redis.zrange('zout', withscores=True)
    assert res == [b'one', 2, b'two', 2, b'three', 3]

    res = yield from redis.zunionstore(
        'zout', 'zset1', 'zset2',
        aggregate=redis.ZSET_AGGREGATE_MAX)
    assert res == 3
    res = yield from redis.zrange('zout', withscores=True)
    assert res == [b'two', 2, b'one', 3, b'three', 3]

    # weights

    with pytest.raises(AssertionError):
        yield from redis.zunionstore('zout', 'zset1', 'zset2',
                                     with_weights=True)

    res = yield from redis.zunionstore('zout',
                                       ('zset1', 2), ('zset2', 2),
                                       with_weights=True)
    assert res == 3
    res = yield from redis.zrange('zout', withscores=True)
    assert res == [b'two', 4, b'three', 6, b'one', 10]


@pytest.mark.run_loop
def test_zrevrangebyscore(redis):
    key = b'key:zrevrangebyscore'
    scores = [1, 1, 2.5, 3, 7]
    members = [b'one', b'uno', b'two', b'three', b'seven']
    pairs = list(itertools.chain(*zip(scores, members)))
    rev_pairs = list(itertools.chain(*zip(members[::-1], scores[::-1])))
    res = yield from redis.zadd(key, *pairs)
    assert res == 5
    res = yield from redis.zrevrangebyscore(key, 7, 1, withscores=False)
    assert res == members[::-1]
    res = yield from redis.zrevrangebyscore(
        key, 7, 1, withscores=False,
        exclude=redis.ZSET_EXCLUDE_BOTH)
    assert res == members[-2:1:-1]
    res = yield from redis.zrevrangebyscore(key, 7, 1, withscores=True)
    assert res == rev_pairs

    res = yield from redis.zrevrangebyscore(key, 10, 1, offset=2, count=2)
    assert res == members[-3:-5:-1]

    with pytest.raises(TypeError):
        yield from redis.zrevrangebyscore(None, 1, 7)
    with pytest.raises(TypeError):
        yield from redis.zrevrangebyscore(key, 10, b'e')
    with pytest.raises(TypeError):
        yield from redis.zrevrangebyscore(key, b'a', 20)
    with pytest.raises(TypeError):
        yield from redis.zrevrangebyscore(key, 1, 7, offset=1)
    with pytest.raises(TypeError):
        yield from redis.zrevrangebyscore(key, 1, 7, count=1)
    with pytest.raises(TypeError):
        yield from redis.zrevrangebyscore(key, 1, 7, offset='one', count=1)
    with pytest.raises(TypeError):
        yield from redis.zrevrangebyscore(key, 1, 7, offset=1, count='one')


@pytest.redis_version(
    2, 8, 9, reason='ZREVRANGEBYLEX is available since redis>=2.8.9')
@pytest.mark.run_loop
def test_zrevrangebylex(redis):
    key = b'key:zrevrangebylex'
    scores = [0] * 5
    members = [b'a', b'b', b'c', b'd', b'e']
    rev_members = members[::-1]
    pairs = list(itertools.chain(*zip(scores, members)))

    res = yield from redis.zadd(key, *pairs)
    assert res == 5
    res = yield from redis.zrevrangebylex(key)
    assert res == rev_members
    res = yield from redis.zrevrangebylex(key, min=b'-', max=b'd')
    assert res == rev_members[1:]
    res = yield from redis.zrevrangebylex(key, min=b'a', max=b'e',
                                          include_min=False,
                                          include_max=False)
    assert res == rev_members[1:-1]
    res = yield from redis.zrevrangebylex(key, min=b'x', max=b'z')
    assert res == []
    res = yield from redis.zrevrangebylex(key, min=b'e', max=b'a')
    assert res == []
    res = yield from redis.zrevrangebylex(key, offset=1, count=2)
    assert res == rev_members[1:3]
    with pytest.raises(TypeError):
        yield from redis.zrevrangebylex(None, b'a', b'e')
    with pytest.raises(TypeError):
        yield from redis.zrevrangebylex(key, 10, b'e')
    with pytest.raises(TypeError):
        yield from redis.zrevrangebylex(key, b'a', 20)
    with pytest.raises(TypeError):
        yield from redis.zrevrangebylex(key, b'a', b'e', offset=1)
    with pytest.raises(TypeError):
        yield from redis.zrevrangebylex(key, b'a', b'e', count=1)
    with pytest.raises(TypeError):
        yield from redis.zrevrangebylex(key, b'a', b'e',
                                        offset='one', count=1)
    with pytest.raises(TypeError):
        yield from redis.zrevrangebylex(key, b'a', b'e',
                                        offset=1, count='one')


@pytest.redis_version(2, 8, 0, reason='ZSCAN is available since redis>=2.8.0')
@pytest.mark.run_loop
def test_zscan(redis):
    key = b'key:zscan'
    scores, members = [], []

    for i in range(1, 11):
        foo_or_bar = 'bar' if i % 3 else 'foo'
        members.append('zmem:{}:{}'.format(foo_or_bar, i).encode('utf-8'))
        scores.append(i)
    pairs = list(itertools.chain(*zip(scores, members)))
    rev_pairs = list(itertools.chain(*zip(members, scores)))
    yield from redis.zadd(key, *pairs)

    cursor, values = yield from redis.zscan(key, match=b'zmem:foo:*')
    assert len(values) == 3 * 2

    cursor, values = yield from redis.zscan(key, match=b'zmem:bar:*')
    assert len(values) == 7 * 2

    # SCAN family functions do not guarantee that the number (count) of
    # elements returned per call are in a given range. So here
    # just dummy test, that *count* argument does not break something
    cursor = b'0'
    test_values = []
    while cursor:
        cursor, values = yield from redis.zscan(key, cursor, count=2)
        test_values.extend(values)
    assert test_values == rev_pairs

    with pytest.raises(TypeError):
        yield from redis.zscan(None)
