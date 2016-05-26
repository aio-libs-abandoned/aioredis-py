import itertools
import pytest


@pytest.redis_version(2, 8, 0, reason='ZSCAN is available since redis>=2.8.0')
@pytest.mark.run_loop
async def test_izscan(redis):
    key = b'key:zscan'
    scores, members = [], []

    for i in range(1, 11):
        foo_or_bar = 'bar' if i % 3 else 'foo'
        members.append('zmem:{}:{}'.format(foo_or_bar, i).encode('utf-8'))
        scores.append(i)
    pairs = list(itertools.chain(*zip(scores, members)))
    await redis.zadd(key, *pairs)
    vals = list(zip(members, scores))

    async def coro(cmd):
        lst = []
        async for i in cmd:
            lst.append(i)
        return lst

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
