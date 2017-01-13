import asyncio
import pytest

from aioredis import ReplyError


@asyncio.coroutine
def add(redis, key, field, value):
    ok = yield from redis.connection.execute(
        b'hset', key, field, value)
    assert ok == 1


@pytest.mark.run_loop
def test_hdel(redis):
    key, field, value = b'key:hdel', b'bar', b'zap'
    yield from add(redis, key, field, value)
    # delete value that exists, expected 1
    result = yield from redis.hdel(key, field)
    assert result == 1
    # delete value that does not exists, expected 0
    result = yield from redis.hdel(key, field)
    assert result == 0

    with pytest.raises(TypeError):
        yield from redis.hdel(None, field)


@pytest.mark.run_loop
def test_hexists(redis):
    key, field, value = b'key:hexists', b'bar', b'zap'
    yield from add(redis, key, field, value)
    # check value that exists, expected 1
    result = yield from redis.hexists(key, field)
    assert result == 1
    # check value when, key exists and field does not, expected 0
    result = yield from redis.hexists(key, b'not:' + field)
    assert result == 0
    # check value when, key not exists, expected 0
    result = yield from redis.hexists(b'not:' + key, field)
    assert result == 0

    with pytest.raises(TypeError):
        yield from redis.hexists(None, field)


@pytest.mark.run_loop
def test_hget(redis):

    key, field, value = b'key:hget', b'bar', b'zap'
    yield from add(redis, key, field, value)
    # basic test, fetch value and check in to reference
    test_value = yield from redis.hget(key, field)
    assert test_value == value
    # fetch value, when field does not exists
    test_value = yield from redis.hget(key, b'not' + field)
    assert test_value is None
    # fetch value when key does not exists
    test_value = yield from redis.hget(b'not:' + key, b'baz')
    assert test_value is None

    # check encoding
    test_value = yield from redis.hget(key, field, encoding='utf-8')
    assert test_value == 'zap'

    with pytest.raises(TypeError):
        yield from redis.hget(None, field)


@pytest.mark.run_loop
def test_hgetall(redis):
    yield from add(redis, 'key:hgetall', 'foo', 'baz')
    yield from add(redis, 'key:hgetall', 'bar', 'zap')

    test_value = yield from redis.hgetall('key:hgetall')
    assert isinstance(test_value, dict)
    assert {b'foo': b'baz', b'bar': b'zap'} == test_value
    # try to get all values from key that does not exits
    test_value = yield from redis.hgetall(b'not:key:hgetall')
    assert test_value == {}

    # check encoding param
    test_value = yield from redis.hgetall(
        'key:hgetall', encoding='utf-8')
    assert {'foo': 'baz', 'bar': 'zap'} == test_value

    with pytest.raises(TypeError):
        yield from redis.hgetall(None)


@pytest.mark.run_loop
def test_hincrby(redis):
    key, field, value = b'key:hincrby', b'bar', 1
    yield from add(redis, key, field, value)
    # increment initial value by 2
    result = yield from redis.hincrby(key, field, 2)
    assert result == 3

    result = yield from redis.hincrby(key, field, -1)
    assert result == 2

    result = yield from redis.hincrby(key, field, -100)
    assert result == -98

    result = yield from redis.hincrby(key, field, -2)
    assert result == -100

    # increment value in case of key or field that does not exists
    result = yield from redis.hincrby(b'not:' + key, field, 2)
    assert result == 2
    result = yield from redis.hincrby(key, b'not:' + field, 2)
    assert result == 2

    with pytest.raises(ReplyError):
        yield from redis.hincrby(key, b'not:' + field, 3.14)

    with pytest.raises(ReplyError):
        # initial value is float, try to increment 1
        yield from add(redis, b'other:' + key, field, 3.14)
        yield from redis.hincrby(b'other:' + key, field, 1)

    with pytest.raises(TypeError):
        yield from redis.hincrby(None, field, 2)


@pytest.mark.run_loop
def test_hincrbyfloat(redis):
    key, field, value = b'key:hincrbyfloat', b'bar', 2.71
    yield from add(redis, key, field, value)

    result = yield from redis.hincrbyfloat(key, field, 3.14)
    assert result == 5.85

    result = yield from redis.hincrbyfloat(key, field, -2.71)
    assert result == 3.14

    result = yield from redis.hincrbyfloat(key, field, -100.1)
    assert result == -96.96

    # increment value in case of key or field that does not exists
    result = yield from redis.hincrbyfloat(b'not:' + key, field, 3.14)
    assert result == 3.14

    result = yield from redis.hincrbyfloat(key, b'not:' + field, 3.14)
    assert result == 3.14

    with pytest.raises(TypeError):
        yield from redis.hincrbyfloat(None, field, 2)


@pytest.mark.run_loop
def test_hkeys(redis):
    key = b'key:hkeys'
    field1, field2 = b'foo', b'bar'
    value1, value2 = b'baz', b'zap'
    yield from add(redis, key, field1, value1)
    yield from add(redis, key, field2, value2)

    test_value = yield from redis.hkeys(key)
    assert set(test_value) == {field1, field2}

    test_value = yield from redis.hkeys(b'not:' + key)
    assert test_value == []

    test_value = yield from redis.hkeys(key, encoding='utf-8')
    assert set(test_value) == {'foo', 'bar'}

    with pytest.raises(TypeError):
        yield from redis.hkeys(None)


@pytest.mark.run_loop
def test_hlen(redis):
    key = b'key:hlen'
    field1, field2 = b'foo', b'bar'
    value1, value2 = b'baz', b'zap'
    yield from add(redis, key, field1, value1)
    yield from add(redis, key, field2, value2)

    test_value = yield from redis.hlen(key)
    assert test_value == 2

    test_value = yield from redis.hlen(b'not:' + key)
    assert test_value == 0

    with pytest.raises(TypeError):
        yield from redis.hlen(None)


@pytest.mark.run_loop
def test_hmget(redis):
    key = b'key:hmget'
    field1, field2 = b'foo', b'bar'
    value1, value2 = b'baz', b'zap'
    yield from add(redis, key, field1, value1)
    yield from add(redis, key, field2, value2)

    test_value = yield from redis.hmget(key, field1, field2)
    assert set(test_value) == {value1, value2}

    test_value = yield from redis.hmget(
        key, b'not:' + field1, b'not:' + field2)
    assert [None, None] == test_value

    val = yield from redis.hincrby(key, 'numeric')
    assert val == 1
    test_value = yield from redis.hmget(
        key, field1, field2, 'numeric', encoding='utf-8')
    assert ['baz', 'zap', '1'] == test_value

    with pytest.raises(TypeError):
        yield from redis.hmget(None, field1, field2)


@pytest.mark.run_loop
def test_hmset(redis):
    key, field, value = b'key:hmset', b'bar', b'zap'
    yield from add(redis, key, field, value)

    # key and field exists
    test_value = yield from redis.hmset(key, field, b'baz')
    assert test_value is True

    result = yield from redis.hexists(key, field)
    assert result == 1

    # key and field does not exists
    test_value = yield from redis.hmset(b'not:' + key, field, value)
    assert test_value is True
    result = yield from redis.hexists(b'not:' + key, field)
    assert result == 1

    # set multiple
    pairs = [b'foo', b'baz', b'bar', b'paz']
    test_value = yield from redis.hmset(key, *pairs)
    assert test_value is True
    test_value = yield from redis.hmget(key, b'foo', b'bar')
    assert set(test_value) == {b'baz', b'paz'}

    with pytest.raises(TypeError):
        yield from redis.hmset(key, b'foo', b'bar', b'baz')

    with pytest.raises(TypeError):
        yield from redis.hmset(None, *pairs)

    with pytest.raises(TypeError):
        yield from redis.hmset(key, {'foo': 'bar'}, {'baz': 'bad'})

    with pytest.raises(TypeError):
        yield from redis.hmset(key)


@pytest.mark.run_loop
def test_hmset_dict(redis):
    key = 'key:hmset'

    # dict
    d1 = {b'foo': b'one dict'}
    test_value = yield from redis.hmset_dict(key, d1)
    assert test_value is True
    test_value = yield from redis.hget(key, b'foo')
    assert test_value == b'one dict'

    # kwdict
    test_value = yield from redis.hmset_dict(key, foo=b'kw1', bar=b'kw2')
    assert test_value is True
    test_value = yield from redis.hmget(key, b'foo', b'bar')
    assert set(test_value) == {b'kw1', b'kw2'}

    # dict & kwdict
    d1 = {b'foo': b'dict'}
    test_value = yield from redis.hmset_dict(key, d1, foo=b'kw')
    assert test_value is True
    test_value = yield from redis.hget(key, b'foo')
    assert test_value == b'kw'

    # allow empty dict with kwargs
    test_value = yield from redis.hmset_dict(key, {}, foo='kw')
    assert test_value is True
    test_value = yield from redis.hget(key, 'foo')
    assert test_value == b'kw'

    with pytest.raises(TypeError):
        yield from redis.hmset_dict(key)

    with pytest.raises(ValueError):
        yield from redis.hmset_dict(key, {})

    with pytest.raises(TypeError):
        yield from redis.hmset_dict(key, ('foo', 'pairs'))

    with pytest.raises(TypeError):
        yield from redis.hmset_dict(key, b'foo', 'pairs')

    with pytest.raises(TypeError):
        yield from redis.hmset_dict(key, b'foo', 'pairs', foo=b'kw1')

    with pytest.raises(TypeError):
        yield from redis.hmset_dict(key, {'a': 1}, {'b': 2})

    with pytest.raises(TypeError):
        yield from redis.hmset_dict(key, {'a': 1}, {'b': 2}, 'c', 3, d=4)


@pytest.mark.run_loop
def test_hset(redis):
    key, field, value = b'key:hset', b'bar', b'zap'
    test_value = yield from redis.hset(key, field, value)
    assert test_value == 1

    test_value = yield from redis.hset(key, field, value)
    assert test_value == 0

    test_value = yield from redis.hset(b'other:' + key, field, value)
    assert test_value == 1

    result = yield from redis.hexists(b'other:' + key, field)
    assert result == 1

    with pytest.raises(TypeError):
        yield from redis.hset(None, field, value)


@pytest.mark.run_loop
def test_hsetnx(redis):
    key, field, value = b'key:hsetnx', b'bar', b'zap'
    # field does not exists, operation should be successful
    test_value = yield from redis.hsetnx(key, field, value)
    assert test_value == 1
    # make sure that value was stored
    result = yield from redis.hget(key, field)
    assert result == value
    # field exists, operation should not change any value
    test_value = yield from redis.hsetnx(key, field, b'baz')
    assert test_value == 0
    # make sure value was not changed
    result = yield from redis.hget(key, field)
    assert result == value

    with pytest.raises(TypeError):
        yield from redis.hsetnx(None, field, value)


@pytest.mark.run_loop
def test_hvals(redis):
    key = b'key:hvals'
    field1, field2 = b'foo', b'bar'
    value1, value2 = b'baz', b'zap'
    yield from add(redis, key, field1, value1)
    yield from add(redis, key, field2, value2)

    test_value = yield from redis.hvals(key)
    assert set(test_value) == {value1, value2}

    test_value = yield from redis.hvals(b'not:' + key)
    assert test_value == []

    test_value = yield from redis.hvals(key, encoding='utf-8')
    assert set(test_value) == {'baz', 'zap'}
    with pytest.raises(TypeError):
        yield from redis.hvals(None)


@pytest.redis_version(2, 8, 0, reason='HSCAN is available since redis>=2.8.0')
@pytest.mark.run_loop
def test_hscan(redis):
    key = b'key:hscan'
    # setup initial values 3 "field:foo:*" items and 7 "field:bar:*" items
    for i in range(1, 11):
        foo_or_bar = 'bar' if i % 3 else 'foo'
        f = 'field:{}:{}'.format(foo_or_bar, i).encode('utf-8')
        v = 'value:{}'.format(i).encode('utf-8')
        yield from add(redis, key, f, v)
    # fetch 'field:foo:*' items expected tuple with 3 fields and 3 values
    cursor, values = yield from redis.hscan(key, match=b'field:foo:*')
    assert len(values) == 3*2
    # fetch 'field:bar:*' items expected tuple with 7 fields and 7 values
    cursor, values = yield from redis.hscan(key, match=b'field:bar:*')
    assert len(values) == 7*2

    # SCAN family functions do not guarantee that the number of
    # elements returned per call are in a given range. So here
    # just dummy test, that *count* argument does not break something
    cursor = b'0'
    test_values = []
    while cursor:
        cursor, values = yield from redis.hscan(key, cursor, count=1)
        test_values.extend(values)
    assert len(test_values) == 10*2

    with pytest.raises(TypeError):
        yield from redis.hscan(None)


@pytest.mark.run_loop
def test_hgetall_enc(create_redis, loop, server):
    redis = yield from create_redis(
        server.tcp_address, loop=loop, encoding='utf-8')
    TEST_KEY = 'my-key-nx'
    yield from redis.hmset(TEST_KEY, 'foo', 'bar', 'baz', 'bad')

    tr = redis.multi_exec()
    tr.hgetall(TEST_KEY)
    res = yield from tr.execute()
    assert res == [{'foo': 'bar', 'baz': 'bad'}]


@pytest.mark.run_loop
@pytest.redis_version(3, 2, 0, reason="HSTRLEN new in redis 3.2.0")
def test_hstrlen(redis):
    ok = yield from redis.hset('myhash', 'str_field', 'some value')
    assert ok == 1
    ok = yield from redis.hincrby('myhash', 'uint_field', 1)
    assert ok == 1

    ok = yield from redis.hincrby('myhash', 'int_field', -1)
    assert ok == -1

    l = yield from redis.hstrlen('myhash', 'str_field')
    assert l == 10
    l = yield from redis.hstrlen('myhash', 'uint_field')
    assert l == 1
    l = yield from redis.hstrlen('myhash', 'int_field')
    assert l == 2

    l = yield from redis.hstrlen('myhash', 'none_field')
    assert l == 0

    l = yield from redis.hstrlen('none_key', 'none_field')
    assert l == 0
