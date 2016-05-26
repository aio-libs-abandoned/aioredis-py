import pytest


@pytest.redis_version(2, 8, 0, reason='SCAN is available since redis>=2.8.0')
@pytest.mark.run_loop
async def test_iscan(redis):
    full = set()
    foo = set()
    bar = set()
    for i in range(1, 11):
        is_bar = i % 3
        foo_or_bar = 'bar' if is_bar else 'foo'
        key = 'key:scan:{}:{}'.format(foo_or_bar, i).encode('utf-8')
        full.add(key)
        if is_bar:
            bar.add(key)
        else:
            foo.add(key)
        assert await redis.set(key, i) is True

    async def coro(cmd):
        lst = []
        async for i in cmd:
            lst.append(i)
        return lst

    ret = await coro(redis.iscan())
    assert len(ret) >= 10

    ret = await coro(redis.iscan(match='key:scan:*'))
    assert 10 == len(ret)
    assert set(ret) == full

    ret = await coro(redis.iscan(match='key:scan:foo*'))
    assert set(ret) == foo

    ret = await coro(redis.iscan(match='key:scan:bar*'))
    assert set(ret) == bar

    # SCAN family functions do not guarantee that the number of
    # elements returned per call are in a given range. So here
    # just dummy test, that *count* argument does not break something

    ret = await coro(redis.iscan(match='key:scan:*', count=2))
    assert 10 == len(ret)
    assert set(ret) == full
