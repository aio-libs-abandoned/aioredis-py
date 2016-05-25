import pytest


@pytest.redis_version(2, 8, 0, reason='HSCAN is available since redis>=2.8.0')
@pytest.mark.run_loop
async def test_ihscan(redis):
    key = b'key:hscan'
    # setup initial values 3 "field:foo:*" items and 7 "field:bar:*" items
    for i in range(1, 11):
        foo_or_bar = 'bar' if i % 3 else 'foo'
        f = 'field:{}:{}'.format(foo_or_bar, i).encode('utf-8')
        v = 'value:{}'.format(i).encode('utf-8')
        assert await redis.hset(key, f, v) == 1

    async def coro(cmd):
        lst = []
        async for i in cmd:
            lst.append(i)
        return lst

    # fetch 'field:foo:*' items expected tuple with 3 fields and 3 values
    ret = await coro(redis.ihscan(key, match=b'field:foo:*'))
    assert set(ret) == {(b'field:foo:3', b'value:3'),
                        (b'field:foo:6', b'value:6'),
                        (b'field:foo:9', b'value:9')}

    # fetch 'field:bar:*' items expected tuple with 7 fields and 7 values
    ret = await coro(redis.ihscan(key, match=b'field:bar:*'))
    assert set(ret) == {(b'field:bar:1', b'value:1'),
                        (b'field:bar:2', b'value:2'),
                        (b'field:bar:4', b'value:4'),
                        (b'field:bar:5', b'value:5'),
                        (b'field:bar:7', b'value:7'),
                        (b'field:bar:8', b'value:8'),
                        (b'field:bar:10', b'value:10')}

    # SCAN family functions do not guarantee that the number of
    # elements returned per call are in a given range. So here
    # just dummy test, that *count* argument does not break something
    ret = await coro(redis.ihscan(key, count=1))
    assert set(ret) == {(b'field:foo:3', b'value:3'),
                        (b'field:foo:6', b'value:6'),
                        (b'field:foo:9', b'value:9'),
                        (b'field:bar:1', b'value:1'),
                        (b'field:bar:2', b'value:2'),
                        (b'field:bar:4', b'value:4'),
                        (b'field:bar:5', b'value:5'),
                        (b'field:bar:7', b'value:7'),
                        (b'field:bar:8', b'value:8'),
                        (b'field:bar:10', b'value:10')}

    with pytest.raises(TypeError):
        await redis.ihscan(None)
