import itertools

import pytest

from _testutils import redis_version


@redis_version(5, 0, 0, reason='BZPOPMAX is available since redis>=5.0.0')
async def test_bzpopmax(redis):
    key1 = b'key:zpopmax:1'
    key2 = b'key:zpopmax:2'

    pairs = [
        (0, b'a'), (5, b'c'), (2, b'd'), (8, b'e'), (9, b'f'), (3, b'g')
    ]
    await redis.zadd(key1, *pairs[0])
    await redis.zadd(key2, *itertools.chain.from_iterable(pairs))

    res = await redis.bzpopmax(key1, timeout=0)
    assert res == [key1, b'a', b'0']
    res = await redis.bzpopmax(key1, key2, timeout=0)
    assert res == [key2, b'f', b'9']

    with pytest.raises(TypeError):
        await redis.bzpopmax(key1, timeout=b'one')
    with pytest.raises(ValueError):
        await redis.bzpopmax(key2, timeout=-10)


@redis_version(5, 0, 0, reason='BZPOPMIN is available since redis>=5.0.0')
async def test_bzpopmin(redis):
    key1 = b'key:zpopmin:1'
    key2 = b'key:zpopmin:2'

    pairs = [
        (0, b'a'), (5, b'c'), (2, b'd'), (8, b'e'), (9, b'f'), (3, b'g')
    ]
    await redis.zadd(key1, *pairs[0])
    await redis.zadd(key2, *itertools.chain.from_iterable(pairs))

    res = await redis.bzpopmin(key1, timeout=0)
    assert res == [key1, b'a', b'0']
    res = await redis.bzpopmin(key1, key2, timeout=0)
    assert res == [key2, b'a', b'0']

    with pytest.raises(TypeError):
        await redis.bzpopmin(key1, timeout=b'one')
    with pytest.raises(ValueError):
        await redis.bzpopmin(key2, timeout=-10)


async def test_zadd(redis):
    key = b'key:zadd'
    res = await redis.zadd(key, 1, b'one')
    assert res == 1
    res = await redis.zadd(key, 1, b'one')
    assert res == 0
    res = await redis.zadd(key, 1, b'uno')
    assert res == 1
    res = await redis.zadd(key, 2.5, b'two')
    assert res == 1
    res = await redis.zadd(key, 3, b'three', 4, b'four')
    assert res == 2

    res = await redis.zrange(key, 0, -1, withscores=False)
    assert res == [b'one', b'uno', b'two', b'three', b'four']

    with pytest.raises(TypeError):
        await redis.zadd(None, 1, b'one')
    with pytest.raises(TypeError):
        await redis.zadd(key, b'two', b'one')
    with pytest.raises(TypeError):
        await redis.zadd(key, 3, b'three', 4)
    with pytest.raises(TypeError):
        await redis.zadd(key, 3, b'three', 'four', 4)


@redis_version(
    3, 0, 2, reason='ZADD options is available since redis>=3.0.2',
)
async def test_zadd_options(redis):
    key = b'key:zaddopt'

    res = await redis.zadd(key, 0, b'one')
    assert res == 1

    res = await redis.zadd(
        key, 1, b'one', 2, b'two',
        exist=redis.ZSET_IF_EXIST,
    )
    assert res == 0

    res = await redis.zscore(key, b'one')
    assert res == 1

    res = await redis.zscore(key, b'two')
    assert res is None

    res = await redis.zadd(
        key, 1, b'one', 2, b'two',
        exist=redis.ZSET_IF_NOT_EXIST,
    )
    assert res == 1

    res = await redis.zscore(key, b'one')
    assert res == 1

    res = await redis.zscore(key, b'two')
    assert res == 2

    res = await redis.zrange(key, 0, -1, withscores=False)
    assert res == [b'one', b'two']

    res = await redis.zadd(key, 1, b'two', changed=True)
    assert res == 1

    res = await redis.zadd(key, 1, b'two', incr=True)
    assert int(res) == 2

    with pytest.raises(ValueError):
        await redis.zadd(key, 1, b'one', 2, b'two', incr=True)


async def test_zcard(redis):
    key = b'key:zcard'
    pairs = [1, b'one', 2, b'two', 3, b'three']
    res = await redis.zadd(key, *pairs)
    assert res == 3
    res = await redis.zcard(key)
    assert res == 3
    res = await redis.zadd(key, 1, b'ein')
    assert res == 1
    res = await redis.zcard(key)
    assert res == 4

    with pytest.raises(TypeError):
        await redis.zcard(None)


async def test_zcount(redis):
    key = b'key:zcount'
    pairs = [1, b'one', 1, b'uno', 2.5, b'two', 3, b'three', 7, b'seven']
    res = await redis.zadd(key, *pairs)
    assert res == 5

    res_zcount = await redis.zcount(key)
    res_zcard = await redis.zcard(key)
    assert res_zcount == res_zcard

    res = await redis.zcount(key, 1, 3)
    assert res == 4
    res = await redis.zcount(key, 3, 10)
    assert res == 2
    res = await redis.zcount(key, 100, 200)
    assert res == 0

    res = await redis.zcount(
        key, 1, 3, exclude=redis.ZSET_EXCLUDE_BOTH)
    assert res == 1
    res = await redis.zcount(
        key, 1, 3, exclude=redis.ZSET_EXCLUDE_MIN)
    assert res == 2
    res = await redis.zcount(
        key, 1, 3, exclude=redis.ZSET_EXCLUDE_MAX)
    assert res == 3
    res = await redis.zcount(
        key, 1, exclude=redis.ZSET_EXCLUDE_MAX)
    assert res == 5
    res = await redis.zcount(
        key, float('-inf'), 3, exclude=redis.ZSET_EXCLUDE_MIN)
    assert res == 4

    with pytest.raises(TypeError):
        await redis.zcount(None)
    with pytest.raises(TypeError):
        await redis.zcount(key, 'one', 2)
    with pytest.raises(TypeError):
        await redis.zcount(key, 1.1, b'two')
    with pytest.raises(ValueError):
        await redis.zcount(key, 10, 1)


async def test_zincrby(redis):
    key = b'key:zincrby'
    pairs = [1, b'one', 1, b'uno', 2.5, b'two', 3, b'three']
    res = await redis.zadd(key, *pairs)
    res = await redis.zincrby(key, 1, b'one')
    assert res == 2
    res = await redis.zincrby(key, -5, b'uno')
    assert res == -4
    res = await redis.zincrby(key, 3.14, b'two')
    assert abs(res - 5.64) <= 0.00001
    res = await redis.zincrby(key, -3.14, b'three')
    assert abs(res - -0.14) <= 0.00001

    with pytest.raises(TypeError):
        await redis.zincrby(None, 5, 'one')
    with pytest.raises(TypeError):
        await redis.zincrby(key, 'one', 5)


async def test_zinterstore(redis):
    zset1 = [2, 'one', 2, 'two']
    zset2 = [3, 'one', 3, 'three']

    await redis.zadd('zset1', *zset1)
    await redis.zadd('zset2', *zset2)

    res = await redis.zinterstore('zout', 'zset1', 'zset2')
    assert res == 1
    res = await redis.zrange('zout', withscores=True)
    assert res == [(b'one', 5)]

    res = await redis.zinterstore(
        'zout', 'zset1', 'zset2',
        aggregate=redis.ZSET_AGGREGATE_SUM)
    assert res == 1
    res = await redis.zrange('zout', withscores=True)
    assert res == [(b'one', 5)]

    res = await redis.zinterstore(
        'zout', 'zset1', 'zset2',
        aggregate=redis.ZSET_AGGREGATE_MIN)
    assert res == 1
    res = await redis.zrange('zout', withscores=True)
    assert res == [(b'one', 2)]

    res = await redis.zinterstore(
        'zout', 'zset1', 'zset2',
        aggregate=redis.ZSET_AGGREGATE_MAX)
    assert res == 1
    res = await redis.zrange('zout', withscores=True)
    assert res == [(b'one', 3)]

    # weights

    with pytest.raises(AssertionError):
        await redis.zinterstore('zout', 'zset1', 'zset2',
                                with_weights=True)

    res = await redis.zinterstore('zout',
                                  ('zset1', 2), ('zset2', 2),
                                  with_weights=True)
    assert res == 1
    res = await redis.zrange('zout', withscores=True)
    assert res == [(b'one', 10)]


@redis_version(
    2, 8, 9, reason='ZLEXCOUNT is available since redis>=2.8.9')
async def test_zlexcount(redis):
    key = b'key:zlexcount'
    pairs = [0, b'a', 0, b'b', 0, b'c', 0, b'd', 0, b'e']
    res = await redis.zadd(key, *pairs)
    assert res == 5
    res = await redis.zlexcount(key)
    assert res == 5
    res = await redis.zlexcount(key, min=b'-', max=b'e')
    assert res == 5
    res = await redis.zlexcount(key, min=b'a', max=b'e',
                                include_min=False,
                                include_max=False)
    assert res == 3

    with pytest.raises(TypeError):
        await redis.zlexcount(None, b'a', b'e')
    with pytest.raises(TypeError):
        await redis.zlexcount(key, 10, b'e')
    with pytest.raises(TypeError):
        await redis.zlexcount(key, b'a', 20)


@pytest.mark.parametrize('encoding', [None, 'utf-8'])
async def test_zrange(redis, encoding):
    key = b'key:zrange'
    scores = [1, 1, 2.5, 3, 7]
    if encoding:
        members = ['one', 'uno', 'two', 'three', 'seven']
    else:
        members = [b'one', b'uno', b'two', b'three', b'seven']
    pairs = list(itertools.chain(*zip(scores, members)))
    rev_pairs = list(zip(members, scores))

    res = await redis.zadd(key, *pairs)
    assert res == 5

    res = await redis.zrange(key, 0, -1, withscores=False, encoding=encoding)
    assert res == members
    res = await redis.zrange(key, 0, -1, withscores=True, encoding=encoding)
    assert res == rev_pairs
    res = await redis.zrange(key, -2, -1, withscores=False, encoding=encoding)
    assert res == members[-2:]
    res = await redis.zrange(key, 1, 2, withscores=False, encoding=encoding)
    assert res == members[1:3]

    with pytest.raises(TypeError):
        await redis.zrange(None, 1, b'one')
    with pytest.raises(TypeError):
        await redis.zrange(key, b'first', -1)
    with pytest.raises(TypeError):
        await redis.zrange(key, 0, 'last')


@redis_version(
    2, 8, 9, reason='ZRANGEBYLEX is available since redis>=2.8.9')
async def test_zrangebylex(redis):
    key = b'key:zrangebylex'
    scores = [0] * 5
    members = [b'a', b'b', b'c', b'd', b'e']
    strings = [x.decode('utf-8') for x in members]
    pairs = list(itertools.chain(*zip(scores, members)))

    res = await redis.zadd(key, *pairs)
    assert res == 5
    res = await redis.zrangebylex(key)
    assert res == members
    res = await redis.zrangebylex(key, encoding='utf-8')
    assert res == strings
    res = await redis.zrangebylex(key, min=b'-', max=b'd')
    assert res == members[:-1]
    res = await redis.zrangebylex(key, min=b'a', max=b'e',
                                  include_min=False,
                                  include_max=False)
    assert res == members[1:-1]
    res = await redis.zrangebylex(key, min=b'x', max=b'z')
    assert res == []
    res = await redis.zrangebylex(key, min=b'e', max=b'a')
    assert res == []
    res = await redis.zrangebylex(key, offset=1, count=2)
    assert res == members[1:3]
    with pytest.raises(TypeError):
        await redis.zrangebylex(None, b'a', b'e')
    with pytest.raises(TypeError):
        await redis.zrangebylex(key, 10, b'e')
    with pytest.raises(TypeError):
        await redis.zrangebylex(key, b'a', 20)
    with pytest.raises(TypeError):
        await redis.zrangebylex(key, b'a', b'e', offset=1)
    with pytest.raises(TypeError):
        await redis.zrangebylex(key, b'a', b'e', count=1)
    with pytest.raises(TypeError):
        await redis.zrangebylex(key, b'a', b'e',
                                     offset='one', count=1)
    with pytest.raises(TypeError):
        await redis.zrangebylex(key, b'a', b'e',
                                     offset=1, count='one')


async def test_zrank(redis):
    key = b'key:zrank'
    scores = [1, 1, 2.5, 3, 7]
    members = [b'one', b'uno', b'two', b'three', b'seven']
    pairs = list(itertools.chain(*zip(scores, members)))

    res = await redis.zadd(key, *pairs)
    assert res == 5

    for i, m in enumerate(members):
        res = await redis.zrank(key, m)
        assert res == i

    res = await redis.zrank(key, b'not:exists')
    assert res is None

    with pytest.raises(TypeError):
        await redis.zrank(None, b'one')


@pytest.mark.parametrize('encoding', [None, 'utf-8'])
async def test_zrangebyscore(redis, encoding):
    key = b'key:zrangebyscore'
    scores = [1, 1, 2.5, 3, 7]
    if encoding:
        members = ['one', 'uno', 'two', 'three', 'seven']
    else:
        members = [b'one', b'uno', b'two', b'three', b'seven']
    pairs = list(itertools.chain(*zip(scores, members)))
    rev_pairs = list(zip(members, scores))
    res = await redis.zadd(key, *pairs)
    assert res == 5

    res = await redis.zrangebyscore(key, 1, 7, withscores=False,
                                    encoding=encoding)
    assert res == members
    res = await redis.zrangebyscore(
        key, 1, 7, withscores=False, exclude=redis.ZSET_EXCLUDE_BOTH,
        encoding=encoding)
    assert res == members[2:-1]
    res = await redis.zrangebyscore(key, 1, 7, withscores=True,
                                    encoding=encoding)
    assert res == rev_pairs

    res = await redis.zrangebyscore(key, 1, 10, offset=2, count=2,
                                    encoding=encoding)
    assert res == members[2:4]

    with pytest.raises(TypeError):
        await redis.zrangebyscore(None, 1, 7)
    with pytest.raises(TypeError):
        await redis.zrangebyscore(key, 10, b'e')
    with pytest.raises(TypeError):
        await redis.zrangebyscore(key, b'a', 20)
    with pytest.raises(TypeError):
        await redis.zrangebyscore(key, 1, 7, offset=1)
    with pytest.raises(TypeError):
        await redis.zrangebyscore(key, 1, 7, count=1)
    with pytest.raises(TypeError):
        await redis.zrangebyscore(key, 1, 7, offset='one', count=1)
    with pytest.raises(TypeError):
        await redis.zrangebyscore(key, 1, 7, offset=1, count='one')


async def test_zrem(redis):
    key = b'key:zrem'
    scores = [1, 1, 2.5, 3, 7]
    members = [b'one', b'uno', b'two', b'three', b'seven']
    pairs = list(itertools.chain(*zip(scores, members)))

    res = await redis.zadd(key, *pairs)
    assert res == 5

    res = await redis.zrem(key, b'uno', b'one')
    assert res == 2

    res = await redis.zrange(key, 0, -1)
    assert res == members[2:]

    res = await redis.zrem(key, b'not:exists')
    assert res == 0

    res = await redis.zrem(b'not:' + key, b'not:exists')
    assert res == 0

    with pytest.raises(TypeError):
        await redis.zrem(None, b'one')


@redis_version(
    2, 8, 9, reason='ZREMRANGEBYLEX is available since redis>=2.8.9')
async def test_zremrangebylex(redis):
    key = b'key:zremrangebylex'
    members = [b'aaaa', b'b', b'c', b'd', b'e', b'foo', b'zap', b'zip',
               b'ALPHA', b'alpha']
    scores = [0] * len(members)

    pairs = list(itertools.chain(*zip(scores, members)))
    res = await redis.zadd(key, *pairs)
    assert res == 10

    res = await redis.zremrangebylex(key, b'alpha', b'omega',
                                     include_max=True,
                                     include_min=True)
    assert res == 6
    res = await redis.zrange(key, 0, -1)
    assert res == [b'ALPHA', b'aaaa', b'zap', b'zip']

    res = await redis.zremrangebylex(key, b'zap', b'zip',
                                     include_max=False,
                                     include_min=False)
    assert res == 0

    res = await redis.zrange(key, 0, -1)
    assert res == [b'ALPHA', b'aaaa', b'zap', b'zip']

    res = await redis.zremrangebylex(key)
    assert res == 4
    res = await redis.zrange(key, 0, -1)
    assert res == []

    with pytest.raises(TypeError):
        await redis.zremrangebylex(None, b'a', b'e')
    with pytest.raises(TypeError):
        await redis.zremrangebylex(key, 10, b'e')
    with pytest.raises(TypeError):
        await redis.zremrangebylex(key, b'a', 20)


async def test_zremrangebyrank(redis):
    key = b'key:zremrangebyrank'
    scores = [0, 1, 2, 3, 4, 5]
    members = [b'zero', b'one', b'two', b'three', b'four', b'five']
    pairs = list(itertools.chain(*zip(scores, members)))
    res = await redis.zadd(key, *pairs)
    assert res == 6

    res = await redis.zremrangebyrank(key, 0, 1)
    assert res == 2
    res = await redis.zrange(key, 0, -1)
    assert res == members[2:]

    res = await redis.zremrangebyrank(key, -2, -1)
    assert res == 2
    res = await redis.zrange(key, 0, -1)
    assert res == members[2:-2]

    with pytest.raises(TypeError):
        await redis.zremrangebyrank(None, 1, 2)
    with pytest.raises(TypeError):
        await redis.zremrangebyrank(key, b'first', -1)
    with pytest.raises(TypeError):
        await redis.zremrangebyrank(key, 0, 'last')


async def test_zremrangebyscore(redis):
    key = b'key:zremrangebyscore'
    scores = [1, 1, 2.5, 3, 7]
    members = [b'one', b'uno', b'two', b'three', b'seven']
    pairs = list(itertools.chain(*zip(scores, members)))
    res = await redis.zadd(key, *pairs)
    assert res == 5

    res = await redis.zremrangebyscore(
        key, 3, 7.5, exclude=redis.ZSET_EXCLUDE_MIN)
    assert res == 1
    res = await redis.zrange(key, 0, -1)
    assert res == members[:-1]

    res = await redis.zremrangebyscore(
        key, 1, 3, exclude=redis.ZSET_EXCLUDE_BOTH)
    assert res == 1
    res = await redis.zrange(key, 0, -1)
    assert res == [b'one', b'uno', b'three']

    res = await redis.zremrangebyscore(key)
    assert res == 3
    res = await redis.zrange(key, 0, -1)
    assert res == []

    with pytest.raises(TypeError):
        await redis.zremrangebyscore(None, 1, 2)
    with pytest.raises(TypeError):
        await redis.zremrangebyscore(key, b'first', -1)
    with pytest.raises(TypeError):
        await redis.zremrangebyscore(key, 0, 'last')


@pytest.mark.parametrize('encoding', [None, 'utf-8'])
async def test_zrevrange(redis, encoding):
    key = b'key:zrevrange'
    scores = [1, 1, 2.5, 3, 7]
    if encoding:
        members = ['one', 'uno', 'two', 'three', 'seven']
    else:
        members = [b'one', b'uno', b'two', b'three', b'seven']
    pairs = list(itertools.chain(*zip(scores, members)))
    rev_pairs = list(zip(members, scores))

    res = await redis.zadd(key, *pairs)
    assert res == 5

    res = await redis.zrevrange(key, 0, -1, withscores=False,
                                encoding=encoding)
    assert res == members[::-1]
    res = await redis.zrevrange(key, 0, -1, withscores=True,
                                encoding=encoding)
    assert res == rev_pairs[::-1]
    res = await redis.zrevrange(key, -2, -1, withscores=False,
                                encoding=encoding)
    assert res == members[1::-1]
    res = await redis.zrevrange(key, 1, 2, withscores=False,
                                encoding=encoding)
    assert res == members[3:1:-1]

    with pytest.raises(TypeError):
        await redis.zrevrange(None, 1, b'one')
    with pytest.raises(TypeError):
        await redis.zrevrange(key, b'first', -1)
    with pytest.raises(TypeError):
        await redis.zrevrange(key, 0, 'last')


async def test_zrevrank(redis):
    key = b'key:zrevrank'
    scores = [1, 1, 2.5, 3, 7]
    members = [b'one', b'uno', b'two', b'three', b'seven']
    pairs = list(itertools.chain(*zip(scores, members)))

    res = await redis.zadd(key, *pairs)
    assert res == 5

    for i, m in enumerate(members):
        res = await redis.zrevrank(key, m)
        assert res == len(members) - i - 1

    res = await redis.zrevrank(key, b'not:exists')
    assert res is None

    with pytest.raises(TypeError):
        await redis.zrevrank(None, b'one')


async def test_zscore(redis):
    key = b'key:zscore'
    scores = [1, 1, 2.5, 3, 7]
    members = [b'one', b'uno', b'two', b'three', b'seven']
    pairs = list(itertools.chain(*zip(scores, members)))

    res = await redis.zadd(key, *pairs)
    assert res == 5

    for s, m in zip(scores, members):
        res = await redis.zscore(key, m)
        assert res == s
    with pytest.raises(TypeError):
        await redis.zscore(None, b'one')
    # Check None on undefined members
    res = await redis.zscore(key, "undefined")
    assert res is None


async def test_zunionstore(redis):
    zset1 = [2, 'one', 2, 'two']
    zset2 = [3, 'one', 3, 'three']

    await redis.zadd('zset1', *zset1)
    await redis.zadd('zset2', *zset2)

    res = await redis.zunionstore('zout', 'zset1', 'zset2')
    assert res == 3
    res = await redis.zrange('zout', withscores=True)
    assert res == [(b'two', 2), (b'three', 3), (b'one', 5)]

    res = await redis.zunionstore(
        'zout', 'zset1', 'zset2',
        aggregate=redis.ZSET_AGGREGATE_SUM)
    assert res == 3
    res = await redis.zrange('zout', withscores=True)
    assert res == [(b'two', 2), (b'three', 3), (b'one', 5)]

    res = await redis.zunionstore(
        'zout', 'zset1', 'zset2',
        aggregate=redis.ZSET_AGGREGATE_MIN)
    assert res == 3
    res = await redis.zrange('zout', withscores=True)
    assert res == [(b'one', 2), (b'two', 2), (b'three', 3)]

    res = await redis.zunionstore(
        'zout', 'zset1', 'zset2',
        aggregate=redis.ZSET_AGGREGATE_MAX)
    assert res == 3
    res = await redis.zrange('zout', withscores=True)
    assert res == [(b'two', 2), (b'one', 3), (b'three', 3)]

    # weights

    with pytest.raises(AssertionError):
        await redis.zunionstore('zout', 'zset1', 'zset2',
                                with_weights=True)

    res = await redis.zunionstore('zout',
                                  ('zset1', 2), ('zset2', 2),
                                  with_weights=True)
    assert res == 3
    res = await redis.zrange('zout', withscores=True)
    assert res == [(b'two', 4), (b'three', 6), (b'one', 10)]


@pytest.mark.parametrize('encoding', [None, 'utf-8'])
async def test_zrevrangebyscore(redis, encoding):
    key = b'key:zrevrangebyscore'
    scores = [1, 1, 2.5, 3, 7]
    if encoding:
        members = ['one', 'uno', 'two', 'three', 'seven']
    else:
        members = [b'one', b'uno', b'two', b'three', b'seven']
    pairs = list(itertools.chain(*zip(scores, members)))
    rev_pairs = list(zip(members[::-1], scores[::-1]))
    res = await redis.zadd(key, *pairs)
    assert res == 5

    res = await redis.zrevrangebyscore(key, 7, 1, withscores=False,
                                       encoding=encoding)
    assert res == members[::-1]
    res = await redis.zrevrangebyscore(
        key, 7, 1, withscores=False,
        exclude=redis.ZSET_EXCLUDE_BOTH,
        encoding=encoding)
    assert res == members[-2:1:-1]
    res = await redis.zrevrangebyscore(key, 7, 1, withscores=True,
                                       encoding=encoding)
    assert res == rev_pairs

    res = await redis.zrevrangebyscore(key, 10, 1, offset=2, count=2,
                                       encoding=encoding)
    assert res == members[-3:-5:-1]

    with pytest.raises(TypeError):
        await redis.zrevrangebyscore(None, 1, 7)
    with pytest.raises(TypeError):
        await redis.zrevrangebyscore(key, 10, b'e')
    with pytest.raises(TypeError):
        await redis.zrevrangebyscore(key, b'a', 20)
    with pytest.raises(TypeError):
        await redis.zrevrangebyscore(key, 1, 7, offset=1)
    with pytest.raises(TypeError):
        await redis.zrevrangebyscore(key, 1, 7, count=1)
    with pytest.raises(TypeError):
        await redis.zrevrangebyscore(key, 1, 7, offset='one', count=1)
    with pytest.raises(TypeError):
        await redis.zrevrangebyscore(key, 1, 7, offset=1, count='one')


@redis_version(
    2, 8, 9, reason='ZREVRANGEBYLEX is available since redis>=2.8.9')
async def test_zrevrangebylex(redis):
    key = b'key:zrevrangebylex'
    scores = [0] * 5
    members = [b'a', b'b', b'c', b'd', b'e']
    strings = [x.decode('utf-8') for x in members]
    rev_members = members[::-1]
    rev_strings = strings[::-1]
    pairs = list(itertools.chain(*zip(scores, members)))

    res = await redis.zadd(key, *pairs)
    assert res == 5
    res = await redis.zrevrangebylex(key)
    assert res == rev_members
    res = await redis.zrevrangebylex(key, encoding='utf-8')
    assert res == rev_strings
    res = await redis.zrevrangebylex(key, min=b'-', max=b'd')
    assert res == rev_members[1:]
    res = await redis.zrevrangebylex(key, min=b'a', max=b'e',
                                     include_min=False,
                                     include_max=False)
    assert res == rev_members[1:-1]
    res = await redis.zrevrangebylex(key, min=b'x', max=b'z')
    assert res == []
    res = await redis.zrevrangebylex(key, min=b'e', max=b'a')
    assert res == []
    res = await redis.zrevrangebylex(key, offset=1, count=2)
    assert res == rev_members[1:3]
    with pytest.raises(TypeError):
        await redis.zrevrangebylex(None, b'a', b'e')
    with pytest.raises(TypeError):
        await redis.zrevrangebylex(key, 10, b'e')
    with pytest.raises(TypeError):
        await redis.zrevrangebylex(key, b'a', 20)
    with pytest.raises(TypeError):
        await redis.zrevrangebylex(key, b'a', b'e', offset=1)
    with pytest.raises(TypeError):
        await redis.zrevrangebylex(key, b'a', b'e', count=1)
    with pytest.raises(TypeError):
        await redis.zrevrangebylex(key, b'a', b'e',
                                   offset='one', count=1)
    with pytest.raises(TypeError):
        await redis.zrevrangebylex(key, b'a', b'e',
                                   offset=1, count='one')


@redis_version(2, 8, 0, reason='ZSCAN is available since redis>=2.8.0')
async def test_zscan(redis):
    key = b'key:zscan'
    scores, members = [], []

    for i in range(1, 11):
        foo_or_bar = 'bar' if i % 3 else 'foo'
        members.append('zmem:{}:{}'.format(foo_or_bar, i).encode('utf-8'))
        scores.append(i)
    pairs = list(itertools.chain(*zip(scores, members)))
    rev_pairs = set(zip(members, scores))
    await redis.zadd(key, *pairs)

    cursor, values = await redis.zscan(key, match=b'zmem:foo:*')
    assert len(values) == 3

    cursor, values = await redis.zscan(key, match=b'zmem:bar:*')
    assert len(values) == 7

    # SCAN family functions do not guarantee that the number (count) of
    # elements returned per call are in a given range. So here
    # just dummy test, that *count* argument does not break something
    cursor = b'0'
    test_values = set()
    while cursor:
        cursor, values = await redis.zscan(key, cursor, count=2)
        test_values.update(values)
    assert test_values == rev_pairs

    with pytest.raises(TypeError):
        await redis.zscan(None)


@redis_version(2, 8, 0, reason='ZSCAN is available since redis>=2.8.0')
async def test_izscan(redis):
    key = b'key:zscan'
    scores, members = [], []

    for i in range(1, 11):
        foo_or_bar = 'bar' if i % 3 else 'foo'
        members.append('zmem:{}:{}'.format(foo_or_bar, i).encode('utf-8'))
        scores.append(i)
    pairs = list(itertools.chain(*zip(scores, members)))
    await redis.zadd(key, *pairs)
    vals = set(zip(members, scores))

    async def coro(cmd):
        res = set()
        async for key, score in cmd:
            res.add((key, score))
        return res

    ret = await coro(redis.izscan(key))
    assert set(ret) == set(vals)

    ret = await coro(redis.izscan(key, match=b'zmem:foo:*'))
    assert set(ret) == set(v for v in vals if b'foo' in v[0])

    ret = await coro(redis.izscan(key, match=b'zmem:bar:*'))
    assert set(ret) == set(v for v in vals if b'bar' in v[0])

    # SCAN family functions do not guarantee that the number (count) of
    # elements returned per call are in a given range. So here
    # just dummy test, that *count* argument does not break something

    ret = await coro(redis.izscan(key, count=2))
    assert set(ret) == set(vals)

    with pytest.raises(TypeError):
        await redis.izscan(None)


@redis_version(5, 0, 0, reason='ZPOPMAX is available since redis>=5.0.0')
async def test_zpopmax(redis):
    key = b'key:zpopmax'

    pairs = [
        (0, b'a'), (5, b'c'), (2, b'd'), (8, b'e'), (9, b'f'), (3, b'g')
    ]
    await redis.zadd(key, *itertools.chain.from_iterable(pairs))

    assert await redis.zpopmax(key) == [b'f', b'9']
    assert await redis.zpopmax(key, 3) == [b'e', b'8', b'c', b'5', b'g', b'3']

    with pytest.raises(TypeError):
        await redis.zpopmax(key, b'b')


@redis_version(5, 0, 0, reason='ZPOPMIN is available since redis>=5.0.0')
async def test_zpopmin(redis):
    key = b'key:zpopmin'

    pairs = [
        (0, b'a'), (5, b'c'), (2, b'd'), (8, b'e'), (9, b'f'), (3, b'g')
    ]
    await redis.zadd(key, *itertools.chain.from_iterable(pairs))

    assert await redis.zpopmin(key) == [b'a', b'0']
    assert await redis.zpopmin(key, 3) == [b'd', b'2', b'g', b'3', b'c', b'5']

    with pytest.raises(TypeError):
        await redis.zpopmin(key, b'b')
