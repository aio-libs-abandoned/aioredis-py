import pytest

from aioredis import ReplyError
from _testutils import redis_version


async def add(redis, key, field, value):
    ok = await redis.connection.execute(
        b'hset', key, field, value)
    assert ok == 1


async def test_hdel(redis):
    key, field, value = b'key:hdel', b'bar', b'zap'
    await add(redis, key, field, value)
    # delete value that exists, expected 1
    result = await redis.hdel(key, field)
    assert result == 1
    # delete value that does not exists, expected 0
    result = await redis.hdel(key, field)
    assert result == 0

    with pytest.raises(TypeError):
        await redis.hdel(None, field)


async def test_hexists(redis):
    key, field, value = b'key:hexists', b'bar', b'zap'
    await add(redis, key, field, value)
    # check value that exists, expected 1
    result = await redis.hexists(key, field)
    assert result == 1
    # check value when, key exists and field does not, expected 0
    result = await redis.hexists(key, b'not:' + field)
    assert result == 0
    # check value when, key not exists, expected 0
    result = await redis.hexists(b'not:' + key, field)
    assert result == 0

    with pytest.raises(TypeError):
        await redis.hexists(None, field)


async def test_hget(redis):

    key, field, value = b'key:hget', b'bar', b'zap'
    await add(redis, key, field, value)
    # basic test, fetch value and check in to reference
    test_value = await redis.hget(key, field)
    assert test_value == value
    # fetch value, when field does not exists
    test_value = await redis.hget(key, b'not' + field)
    assert test_value is None
    # fetch value when key does not exists
    test_value = await redis.hget(b'not:' + key, b'baz')
    assert test_value is None

    # check encoding
    test_value = await redis.hget(key, field, encoding='utf-8')
    assert test_value == 'zap'

    with pytest.raises(TypeError):
        await redis.hget(None, field)


async def test_hgetall(redis):
    await add(redis, 'key:hgetall', 'foo', 'baz')
    await add(redis, 'key:hgetall', 'bar', 'zap')

    test_value = await redis.hgetall('key:hgetall')
    assert isinstance(test_value, dict)
    assert {b'foo': b'baz', b'bar': b'zap'} == test_value
    # try to get all values from key that does not exits
    test_value = await redis.hgetall(b'not:key:hgetall')
    assert test_value == {}

    # check encoding param
    test_value = await redis.hgetall(
        'key:hgetall', encoding='utf-8')
    assert {'foo': 'baz', 'bar': 'zap'} == test_value

    with pytest.raises(TypeError):
        await redis.hgetall(None)


async def test_hincrby(redis):
    key, field, value = b'key:hincrby', b'bar', 1
    await add(redis, key, field, value)
    # increment initial value by 2
    result = await redis.hincrby(key, field, 2)
    assert result == 3

    result = await redis.hincrby(key, field, -1)
    assert result == 2

    result = await redis.hincrby(key, field, -100)
    assert result == -98

    result = await redis.hincrby(key, field, -2)
    assert result == -100

    # increment value in case of key or field that does not exists
    result = await redis.hincrby(b'not:' + key, field, 2)
    assert result == 2
    result = await redis.hincrby(key, b'not:' + field, 2)
    assert result == 2

    with pytest.raises(ReplyError):
        await redis.hincrby(key, b'not:' + field, 3.14)

    with pytest.raises(ReplyError):
        # initial value is float, try to increment 1
        await add(redis, b'other:' + key, field, 3.14)
        await redis.hincrby(b'other:' + key, field, 1)

    with pytest.raises(TypeError):
        await redis.hincrby(None, field, 2)


async def test_hincrbyfloat(redis):
    key, field, value = b'key:hincrbyfloat', b'bar', 2.71
    await add(redis, key, field, value)

    result = await redis.hincrbyfloat(key, field, 3.14)
    assert result == 5.85

    result = await redis.hincrbyfloat(key, field, -2.71)
    assert result == 3.14

    result = await redis.hincrbyfloat(key, field, -100.1)
    assert result == -96.96

    # increment value in case of key or field that does not exists
    result = await redis.hincrbyfloat(b'not:' + key, field, 3.14)
    assert result == 3.14

    result = await redis.hincrbyfloat(key, b'not:' + field, 3.14)
    assert result == 3.14

    with pytest.raises(TypeError):
        await redis.hincrbyfloat(None, field, 2)


async def test_hkeys(redis):
    key = b'key:hkeys'
    field1, field2 = b'foo', b'bar'
    value1, value2 = b'baz', b'zap'
    await add(redis, key, field1, value1)
    await add(redis, key, field2, value2)

    test_value = await redis.hkeys(key)
    assert set(test_value) == {field1, field2}

    test_value = await redis.hkeys(b'not:' + key)
    assert test_value == []

    test_value = await redis.hkeys(key, encoding='utf-8')
    assert set(test_value) == {'foo', 'bar'}

    with pytest.raises(TypeError):
        await redis.hkeys(None)


async def test_hlen(redis):
    key = b'key:hlen'
    field1, field2 = b'foo', b'bar'
    value1, value2 = b'baz', b'zap'
    await add(redis, key, field1, value1)
    await add(redis, key, field2, value2)

    test_value = await redis.hlen(key)
    assert test_value == 2

    test_value = await redis.hlen(b'not:' + key)
    assert test_value == 0

    with pytest.raises(TypeError):
        await redis.hlen(None)


async def test_hmget(redis):
    key = b'key:hmget'
    field1, field2 = b'foo', b'bar'
    value1, value2 = b'baz', b'zap'
    await add(redis, key, field1, value1)
    await add(redis, key, field2, value2)

    test_value = await redis.hmget(key, field1, field2)
    assert set(test_value) == {value1, value2}

    test_value = await redis.hmget(
        key, b'not:' + field1, b'not:' + field2)
    assert [None, None] == test_value

    val = await redis.hincrby(key, 'numeric')
    assert val == 1
    test_value = await redis.hmget(
        key, field1, field2, 'numeric', encoding='utf-8')
    assert ['baz', 'zap', '1'] == test_value

    with pytest.raises(TypeError):
        await redis.hmget(None, field1, field2)


async def test_hmset(redis):
    key, field, value = b'key:hmset', b'bar', b'zap'
    await add(redis, key, field, value)

    # key and field exists
    test_value = await redis.hmset(key, field, b'baz')
    assert test_value is True

    result = await redis.hexists(key, field)
    assert result == 1

    # key and field does not exists
    test_value = await redis.hmset(b'not:' + key, field, value)
    assert test_value is True
    result = await redis.hexists(b'not:' + key, field)
    assert result == 1

    # set multiple
    pairs = [b'foo', b'baz', b'bar', b'paz']
    test_value = await redis.hmset(key, *pairs)
    assert test_value is True
    test_value = await redis.hmget(key, b'foo', b'bar')
    assert set(test_value) == {b'baz', b'paz'}

    with pytest.raises(TypeError):
        await redis.hmset(key, b'foo', b'bar', b'baz')

    with pytest.raises(TypeError):
        await redis.hmset(None, *pairs)

    with pytest.raises(TypeError):
        await redis.hmset(key, {'foo': 'bar'}, {'baz': 'bad'})

    with pytest.raises(TypeError):
        await redis.hmset(key)


async def test_hmset_dict(redis):
    key = 'key:hmset'

    # dict
    d1 = {b'foo': b'one dict'}
    test_value = await redis.hmset_dict(key, d1)
    assert test_value is True
    test_value = await redis.hget(key, b'foo')
    assert test_value == b'one dict'

    # kwdict
    test_value = await redis.hmset_dict(key, foo=b'kw1', bar=b'kw2')
    assert test_value is True
    test_value = await redis.hmget(key, b'foo', b'bar')
    assert set(test_value) == {b'kw1', b'kw2'}

    # dict & kwdict
    d1 = {b'foo': b'dict'}
    test_value = await redis.hmset_dict(key, d1, foo=b'kw')
    assert test_value is True
    test_value = await redis.hget(key, b'foo')
    assert test_value == b'kw'

    # allow empty dict with kwargs
    test_value = await redis.hmset_dict(key, {}, foo='kw')
    assert test_value is True
    test_value = await redis.hget(key, 'foo')
    assert test_value == b'kw'

    with pytest.raises(TypeError):
        await redis.hmset_dict(key)

    with pytest.raises(ValueError):
        await redis.hmset_dict(key, {})

    with pytest.raises(TypeError):
        await redis.hmset_dict(key, ('foo', 'pairs'))

    with pytest.raises(TypeError):
        await redis.hmset_dict(key, b'foo', 'pairs')

    with pytest.raises(TypeError):
        await redis.hmset_dict(key, b'foo', 'pairs', foo=b'kw1')

    with pytest.raises(TypeError):
        await redis.hmset_dict(key, {'a': 1}, {'b': 2})

    with pytest.raises(TypeError):
        await redis.hmset_dict(key, {'a': 1}, {'b': 2}, 'c', 3, d=4)


async def test_hset(redis):
    key, field, value = b'key:hset', b'bar', b'zap'
    test_value = await redis.hset(key, field, value)
    assert test_value == 1

    test_value = await redis.hset(key, field, value)
    assert test_value == 0

    test_value = await redis.hset(b'other:' + key, field, value)
    assert test_value == 1

    result = await redis.hexists(b'other:' + key, field)
    assert result == 1

    with pytest.raises(TypeError):
        await redis.hset(None, field, value)


async def test_hsetnx(redis):
    key, field, value = b'key:hsetnx', b'bar', b'zap'
    # field does not exists, operation should be successful
    test_value = await redis.hsetnx(key, field, value)
    assert test_value == 1
    # make sure that value was stored
    result = await redis.hget(key, field)
    assert result == value
    # field exists, operation should not change any value
    test_value = await redis.hsetnx(key, field, b'baz')
    assert test_value == 0
    # make sure value was not changed
    result = await redis.hget(key, field)
    assert result == value

    with pytest.raises(TypeError):
        await redis.hsetnx(None, field, value)


async def test_hvals(redis):
    key = b'key:hvals'
    field1, field2 = b'foo', b'bar'
    value1, value2 = b'baz', b'zap'
    await add(redis, key, field1, value1)
    await add(redis, key, field2, value2)

    test_value = await redis.hvals(key)
    assert set(test_value) == {value1, value2}

    test_value = await redis.hvals(b'not:' + key)
    assert test_value == []

    test_value = await redis.hvals(key, encoding='utf-8')
    assert set(test_value) == {'baz', 'zap'}
    with pytest.raises(TypeError):
        await redis.hvals(None)


@redis_version(2, 8, 0, reason='HSCAN is available since redis>=2.8.0')
async def test_hscan(redis):
    key = b'key:hscan'
    # setup initial values 3 "field:foo:*" items and 7 "field:bar:*" items
    for i in range(1, 11):
        foo_or_bar = 'bar' if i % 3 else 'foo'
        f = 'field:{}:{}'.format(foo_or_bar, i).encode('utf-8')
        v = 'value:{}'.format(i).encode('utf-8')
        await add(redis, key, f, v)
    # fetch 'field:foo:*' items expected tuple with 3 fields and 3 values
    cursor, values = await redis.hscan(key, match=b'field:foo:*')
    assert len(values) == 3
    assert sorted(values) == [
        (b'field:foo:3', b'value:3'),
        (b'field:foo:6', b'value:6'),
        (b'field:foo:9', b'value:9'),
        ]
    # fetch 'field:bar:*' items expected tuple with 7 fields and 7 values
    cursor, values = await redis.hscan(key, match=b'field:bar:*')
    assert len(values) == 7
    assert sorted(values) == [
        (b'field:bar:1', b'value:1'),
        (b'field:bar:10', b'value:10'),
        (b'field:bar:2', b'value:2'),
        (b'field:bar:4', b'value:4'),
        (b'field:bar:5', b'value:5'),
        (b'field:bar:7', b'value:7'),
        (b'field:bar:8', b'value:8'),
        ]

    # SCAN family functions do not guarantee that the number of
    # elements returned per call are in a given range. So here
    # just dummy test, that *count* argument does not break something
    cursor = b'0'
    test_values = []
    while cursor:
        cursor, values = await redis.hscan(key, cursor, count=1)
        test_values.extend(values)
    assert len(test_values) == 10

    with pytest.raises(TypeError):
        await redis.hscan(None)


async def test_hgetall_enc(create_redis, server):
    redis = await create_redis(server.tcp_address, encoding='utf-8')
    TEST_KEY = 'my-key-nx'
    await redis.hmset(TEST_KEY, 'foo', 'bar', 'baz', 'bad')

    tr = redis.multi_exec()
    tr.hgetall(TEST_KEY)
    res = await tr.execute()
    assert res == [{'foo': 'bar', 'baz': 'bad'}]


@redis_version(3, 2, 0, reason="HSTRLEN new in redis 3.2.0")
async def test_hstrlen(redis):
    ok = await redis.hset('myhash', 'str_field', 'some value')
    assert ok == 1
    ok = await redis.hincrby('myhash', 'uint_field', 1)
    assert ok == 1

    ok = await redis.hincrby('myhash', 'int_field', -1)
    assert ok == -1

    l = await redis.hstrlen('myhash', 'str_field')
    assert l == 10
    l = await redis.hstrlen('myhash', 'uint_field')
    assert l == 1
    l = await redis.hstrlen('myhash', 'int_field')
    assert l == 2

    l = await redis.hstrlen('myhash', 'none_field')
    assert l == 0

    l = await redis.hstrlen('none_key', 'none_field')
    assert l == 0


@redis_version(2, 8, 0, reason='HSCAN is available since redis>=2.8.0')
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
