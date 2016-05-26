import pytest


@pytest.redis_version(2, 8, 0, reason='SSCAN is available since redis>=2.8.0')
@pytest.mark.run_loop
async def test_isscan(redis):
    key = b'key:sscan'
    for i in range(1, 11):
        foo_or_bar = 'bar' if i % 3 else 'foo'
        member = 'member:{}:{}'.format(foo_or_bar, i).encode('utf-8')
        assert await redis.sadd(key, member) == 1

    async def coro(cmd):
        lst = []
        async for i in cmd:
            lst.append(i)
        return lst

    ret = await coro(redis.isscan(key, match=b'member:foo:*'))
    assert set(ret) == {b'member:foo:3',
                        b'member:foo:6',
                        b'member:foo:9'}

    ret = await coro(redis.isscan(key, match=b'member:bar:*'))
    assert set(ret) == {b'member:bar:1',
                        b'member:bar:2',
                        b'member:bar:4',
                        b'member:bar:5',
                        b'member:bar:7',
                        b'member:bar:8',
                        b'member:bar:10'}

    # SCAN family functions do not guarantee that the number (count) of
    # elements returned per call are in a given range. So here
    # just dummy test, that *count* argument does not break something
    ret = await coro(redis.isscan(key, count=2))
    assert set(ret) == {b'member:foo:3',
                        b'member:foo:6',
                        b'member:foo:9',
                        b'member:bar:1',
                        b'member:bar:2',
                        b'member:bar:4',
                        b'member:bar:5',
                        b'member:bar:7',
                        b'member:bar:8',
                        b'member:bar:10'}

    with pytest.raises(TypeError):
        await redis.isscan(None)
