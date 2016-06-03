import pytest
import asyncio

from aioredis import ReplyError


@asyncio.coroutine
def push_data_with_sleep(redis, loop, key, *values):
    yield from asyncio.sleep(0.2, loop=loop)
    result = yield from redis.lpush(key, *values)
    return result


@pytest.mark.run_loop
def test_blpop(redis):
    key1, value1 = b'key:blpop:1', b'blpop:value:1'
    key2, value2 = b'key:blpop:2', b'blpop:value:2'

    # setup list
    result = yield from redis.rpush(key1, value1, value2)
    assert result == 2
    # make sure that left value poped
    test_value = yield from redis.blpop(key1)
    assert test_value == [key1, value1]
    # pop remaining value, so list should become empty
    test_value = yield from redis.blpop(key1)
    assert test_value == [key1, value2]

    with pytest.raises(TypeError):
        yield from redis.blpop(None)
    with pytest.raises(TypeError):
        yield from redis.blpop(key1, None)
    with pytest.raises(TypeError):
        yield from redis.blpop(key1, timeout=b'one')
    with pytest.raises(ValueError):
        yield from redis.blpop(key2, timeout=-10)

    # test encoding param
    yield from redis.rpush(key2, value1)
    test_value = yield from redis.blpop(key2, encoding='utf-8')
    assert test_value == ['key:blpop:2', 'blpop:value:1']


@pytest.mark.run_loop
def test_blpop_blocking_features(redis, create_redis, loop, server):
    key1, key2 = b'key:blpop:1', b'key:blpop:2'
    value = b'blpop:value:2'

    other_redis = yield from create_redis(
        server.tcp_address, loop=loop)

    # create blocking task in separate connection
    consumer = other_redis.blpop(key1, key2)

    producer_task = asyncio.Task(
        push_data_with_sleep(redis, loop, key2, value), loop=loop)
    results = yield from asyncio.gather(
        consumer, producer_task, loop=loop)

    assert results[0] == [key2, value]
    assert results[1] == 1

    # wait for data with timeout, list is emtpy, so blpop should
    # return None in 1 sec
    waiter = redis.blpop(key1, key2, timeout=1)
    test_value = yield from waiter
    assert test_value is None
    other_redis.close()


@pytest.mark.run_loop
def test_brpop(redis):
    key1, value1 = b'key:brpop:1', b'brpop:value:1'
    key2, value2 = b'key:brpop:2', b'brpop:value:2'

    # setup list
    result = yield from redis.rpush(key1, value1, value2)
    assert result == 2
    # make sure that right value poped
    test_value = yield from redis.brpop(key1)
    assert test_value == [key1, value2]
    # pop remaining value, so list should become empty
    test_value = yield from redis.brpop(key1)
    assert test_value == [key1, value1]

    with pytest.raises(TypeError):
        yield from redis.brpop(None)
    with pytest.raises(TypeError):
        yield from redis.brpop(key1, None)
    with pytest.raises(TypeError):
        yield from redis.brpop(key1, timeout=b'one')
    with pytest.raises(ValueError):
        yield from redis.brpop(key2, timeout=-10)

    # test encoding param
    yield from redis.rpush(key2, value1)
    test_value = yield from redis.brpop(key2, encoding='utf-8')
    assert test_value == ['key:brpop:2', 'brpop:value:1']


@pytest.mark.run_loop
def test_brpop_blocking_features(redis, create_redis, server, loop):
    key1, key2 = b'key:brpop:1', b'key:brpop:2'
    value = b'brpop:value:2'

    other_redis = yield from create_redis(
        server.tcp_address, loop=loop)
    # create blocking task in separate connection
    consumer_task = other_redis.brpop(key1, key2)

    producer_task = asyncio.Task(
        push_data_with_sleep(redis, loop, key2, value), loop=loop)

    results = yield from asyncio.gather(
        consumer_task, producer_task, loop=loop)

    assert results[0] == [key2, value]
    assert results[1] == 1

    # wait for data with timeout, list is emtpy, so brpop should
    # return None in 1 sec
    waiter = redis.brpop(key1, key2, timeout=1)
    test_value = yield from waiter
    assert test_value is None


@pytest.mark.run_loop
def test_brpoplpush(redis):
    key = b'key:brpoplpush:1'
    value1, value2 = b'brpoplpush:value:1', b'brpoplpush:value:2'

    destkey = b'destkey:brpoplpush:1'

    # setup list
    yield from redis.rpush(key, value1, value2)

    # move value in into head of new list
    result = yield from redis.brpoplpush(key, destkey)
    assert result == value2
    # move last value
    result = yield from redis.brpoplpush(key, destkey)
    assert result == value1

    # make sure that all values stored in new destkey list
    test_value = yield from redis.lrange(destkey, 0, -1)
    assert test_value == [value1, value2]

    with pytest.raises(TypeError):
        yield from redis.brpoplpush(None, destkey)

    with pytest.raises(TypeError):
        yield from redis.brpoplpush(key, None)

    with pytest.raises(TypeError):
        yield from redis.brpoplpush(key, destkey, timeout=b'one')

    with pytest.raises(ValueError):
        yield from redis.brpoplpush(key, destkey, timeout=-10)

    # test encoding param
    result = yield from redis.brpoplpush(
        destkey, key, encoding='utf-8')
    assert result == 'brpoplpush:value:2'


@pytest.mark.run_loop
def test_brpoplpush_blocking_features(redis, create_redis, server, loop):
    source = b'key:brpoplpush:12'
    value = b'brpoplpush:value:2'
    destkey = b'destkey:brpoplpush:2'
    other_redis = yield from create_redis(
        server.tcp_address, loop=loop)
    # create blocking task
    consumer_task = other_redis.brpoplpush(source, destkey)
    producer_task = asyncio.Task(
        push_data_with_sleep(redis, loop, source, value), loop=loop)
    results = yield from asyncio.gather(
        consumer_task, producer_task, loop=loop)
    assert results[0] == value
    assert results[1] == 1

    # make sure that all values stored in new destkey list
    test_value = yield from redis.lrange(destkey, 0, -1)
    assert test_value == [value]

    # wait for data with timeout, list is emtpy, so brpoplpush should
    # return None in 1 sec
    waiter = redis.brpoplpush(source, destkey, timeout=1)
    test_value = yield from waiter
    assert test_value is None
    other_redis.close()


@pytest.mark.run_loop
def test_lindex(redis):
    key, value = b'key:lindex:1', 'value:{}'
    # setup list
    values = [value.format(i).encode('utf-8') for i in range(0, 10)]
    yield from redis.rpush(key, *values)
    # make sure that all indexes are correct
    for i in range(0, 10):
        test_value = yield from redis.lindex(key, i)
        assert test_value == values[i]

    # get last element
    test_value = yield from redis.lindex(key, -1)
    assert test_value == b'value:9'

    # index of element if key does not exists
    test_value = yield from redis.lindex(b'not:' + key, 5)
    assert test_value is None

    # test encoding param
    yield from redis.rpush(key, 'one', 'two')
    test_value = yield from redis.lindex(key, 10, encoding='utf-8')
    assert test_value == 'one'
    test_value = yield from redis.lindex(key, 11, encoding='utf-8')
    assert test_value == 'two'

    with pytest.raises(TypeError):
        yield from redis.lindex(None, -1)

    with pytest.raises(TypeError):
        yield from redis.lindex(key, b'one')


@pytest.mark.run_loop
def test_linsert(redis):
    key = b'key:linsert:1'
    value1, value2, value3, value4 = b'Hello', b'World', b'foo', b'bar'
    yield from redis.rpush(key, value1, value2)

    # insert element before pivot
    test_value = yield from redis.linsert(
        key, value2, value3, before=True)
    assert test_value == 3
    # insert element after pivot
    test_value = yield from redis.linsert(
        key, value2, value4, before=False)
    assert test_value == 4

    # make sure that values actually inserted in right placed
    test_value = yield from redis.lrange(key, 0, -1)
    expected = [value1, value3, value2, value4]
    assert test_value == expected

    # try to insert something when pivot value does not exits
    test_value = yield from redis.linsert(
        key, b'not:pivot', value3, before=True)
    assert test_value == -1

    with pytest.raises(TypeError):
        yield from redis.linsert(None, value1, value3)


@pytest.mark.run_loop
def test_llen(redis):
    key = b'key:llen:1'
    value1, value2 = b'Hello', b'World'
    yield from redis.rpush(key, value1, value2)

    test_value = yield from redis.llen(key)
    assert test_value == 2

    test_value = yield from redis.llen(b'not:' + key)
    assert test_value == 0

    with pytest.raises(TypeError):
        yield from redis.llen(None)


@pytest.mark.run_loop
def test_lpop(redis):
    key = b'key:lpop:1'
    value1, value2 = b'lpop:value:1', b'lpop:value:2'

    # setup list
    result = yield from redis.rpush(key, value1, value2)
    assert result == 2
    # make sure that left value poped
    test_value = yield from redis.lpop(key)
    assert test_value == value1
    # pop remaining value, so list should become empty
    test_value = yield from redis.lpop(key)
    assert test_value == value2
    # pop from empty list
    test_value = yield from redis.lpop(key)
    assert test_value is None

    # test encoding param
    yield from redis.rpush(key, 'value')
    test_value = yield from redis.lpop(key, encoding='utf-8')
    assert test_value == 'value'

    with pytest.raises(TypeError):
        yield from redis.lpop(None)


@pytest.mark.run_loop
def test_lpush(redis):
    key = b'key:lpush'
    value1, value2 = b'value:1', b'value:2'

    # add multiple values to the list, with key that does not exists
    result = yield from redis.lpush(key, value1, value2)
    assert result == 2

    # make sure that values actually inserted in right placed and order
    test_value = yield from redis.lrange(key, 0, -1)
    assert test_value == [value2, value1]

    # test encoding param
    test_value = yield from redis.lrange(key, 0, -1, encoding='utf-8')
    assert test_value == ['value:2', 'value:1']

    with pytest.raises(TypeError):
        yield from redis.lpush(None, value1)


@pytest.mark.run_loop
def test_lpushx(redis):
    key = b'key:lpushx'
    value1, value2 = b'value:1', b'value:2'

    # add multiple values to the list, with key that does not exists
    # so value should not be pushed
    result = yield from redis.lpushx(key, value2)
    assert result == 0
    # init key with list by using regular lpush
    result = yield from redis.lpush(key, value1)
    assert result == 1

    result = yield from redis.lpushx(key, value2)
    assert result == 2

    # make sure that values actually inserted in right placed and order
    test_value = yield from redis.lrange(key, 0, -1)
    assert test_value == [value2, value1]

    with pytest.raises(TypeError):
        yield from redis.lpushx(None, value1)


@pytest.mark.run_loop
def test_lrange(redis):
    key, value = b'key:lrange:1', 'value:{}'
    values = [value.format(i).encode('utf-8') for i in range(0, 10)]
    yield from redis.rpush(key, *values)

    test_value = yield from redis.lrange(key, 0, 2)
    assert test_value == values[0:3]

    test_value = yield from redis.lrange(key, 0, -1)
    assert test_value == values

    test_value = yield from redis.lrange(key, -2, -1)
    assert test_value == values[-2:]

    # range of elements if key does not exists
    test_value = yield from redis.lrange(b'not:' + key, 0, -1)
    assert test_value == []

    with pytest.raises(TypeError):
        yield from redis.lrange(None, 0, -1)

    with pytest.raises(TypeError):
        yield from redis.lrange(key, b'zero', -1)

    with pytest.raises(TypeError):
        yield from redis.lrange(key, 0, b'one')


@pytest.mark.run_loop
def test_lrem(redis):
    key, value = b'key:lrem:1', 'value:{}'
    values = [value.format(i % 2).encode('utf-8') for i in range(0, 10)]
    yield from redis.rpush(key, *values)
    # remove elements from tail to head
    test_value = yield from redis.lrem(key, -4, b'value:0')
    assert test_value == 4
    # remove element from head to tail
    test_value = yield from redis.lrem(key, 4, b'value:1')
    assert test_value == 4

    # remove values that not in list
    test_value = yield from redis.lrem(key, 4, b'value:other')
    assert test_value == 0

    # make sure that only two values left in the list
    test_value = yield from redis.lrange(key, 0, -1)
    assert test_value == [b'value:0', b'value:1']

    # remove all instance of value:0
    test_value = yield from redis.lrem(key, 0, b'value:0')
    assert test_value == 1

    # make sure that only one values left in the list
    test_value = yield from redis.lrange(key, 0, -1)
    assert test_value == [b'value:1']

    with pytest.raises(TypeError):
        yield from redis.lrem(None, 0, b'value:0')

    with pytest.raises(TypeError):
        yield from redis.lrem(key, b'ten', b'value:0')


@pytest.mark.run_loop
def test_lset(redis):
    key, value = b'key:lset', 'value:{}'
    values = [value.format(i).encode('utf-8') for i in range(0, 3)]
    yield from redis.rpush(key, *values)

    yield from redis.lset(key, 0, b'foo')
    yield from redis.lset(key, -1, b'baz')
    yield from redis.lset(key, -2, b'zap')

    test_value = yield from redis.lrange(key, 0, -1)
    assert test_value == [b'foo', b'zap', b'baz']

    with pytest.raises(TypeError):
        yield from redis.lset(None, 0, b'value:0')

    with pytest.raises(ReplyError):
        yield from redis.lset(key, 100, b'value:0')

    with pytest.raises(TypeError):
        yield from redis.lset(key, b'one', b'value:0')


@pytest.mark.run_loop
def test_ltrim(redis):
    key, value = b'key:ltrim', 'value:{}'
    values = [value.format(i).encode('utf-8') for i in range(0, 10)]
    yield from redis.rpush(key, *values)

    # trim with negative indexes
    yield from redis.ltrim(key, 0, -5)
    test_value = yield from redis.lrange(key, 0, -1)
    assert test_value == values[:-4]
    # trim with positive indexes
    yield from redis.ltrim(key, 0, 2)
    test_value = yield from redis.lrange(key, 0, -1)
    assert test_value == values[:3]

    # try to trim out of range indexes
    res = yield from redis.ltrim(key, 100, 110)
    assert res is True
    test_value = yield from redis.lrange(key, 0, -1)
    assert test_value == []

    with pytest.raises(TypeError):
        yield from redis.ltrim(None, 0, -1)

    with pytest.raises(TypeError):
        yield from redis.ltrim(key, b'zero', -1)

    with pytest.raises(TypeError):
        yield from redis.ltrim(key, 0, b'one')


@pytest.mark.run_loop
def test_rpop(redis):
    key = b'key:rpop:1'
    value1, value2 = b'rpop:value:1', b'rpop:value:2'

    # setup list
    result = yield from redis.rpush(key, value1, value2)
    assert result == 2
    # make sure that left value poped
    test_value = yield from redis.rpop(key)
    assert test_value == value2
    # pop remaining value, so list should become empty
    test_value = yield from redis.rpop(key)
    assert test_value == value1
    # pop from empty list
    test_value = yield from redis.rpop(key)
    assert test_value is None

    # test encoding param
    yield from redis.rpush(key, 'value')
    test_value = yield from redis.rpop(key, encoding='utf-8')
    assert test_value == 'value'

    with pytest.raises(TypeError):
        yield from redis.rpop(None)


@pytest.mark.run_loop
def test_rpoplpush(redis):
    key = b'key:rpoplpush:1'
    value1, value2 = b'rpoplpush:value:1', b'rpoplpush:value:2'
    destkey = b'destkey:rpoplpush:1'

    # setup list
    yield from redis.rpush(key, value1, value2)

    # move value in into head of new list
    result = yield from redis.rpoplpush(key, destkey)
    assert result == value2
    # move last value
    result = yield from redis.rpoplpush(key, destkey)
    assert result == value1

    # make sure that all values stored in new destkey list
    result = yield from redis.lrange(destkey, 0, -1)
    assert result == [value1, value2]

    # test encoding param
    result = yield from redis.rpoplpush(
        destkey, key, encoding='utf-8')
    assert result == 'rpoplpush:value:2'

    with pytest.raises(TypeError):
        yield from redis.rpoplpush(None, destkey)

    with pytest.raises(TypeError):
        yield from redis.rpoplpush(key, None)


@pytest.mark.run_loop
def test_rpush(redis):
    key = b'key:rpush'
    value1, value2 = b'value:1', b'value:2'

    # add multiple values to the list, with key that does not exists
    result = yield from redis.rpush(key, value1, value2)
    assert result == 2

    # make sure that values actually inserted in right placed and order
    test_value = yield from redis.lrange(key, 0, -1)
    assert test_value == [value1, value2]

    with pytest.raises(TypeError):
        yield from redis.rpush(None, value1)


@pytest.mark.run_loop
def test_rpushx(redis):
    key = b'key:rpushx'
    value1, value2 = b'value:1', b'value:2'

    # add multiple values to the list, with key that does not exists
    # so value should not be pushed
    result = yield from redis.rpushx(key, value2)
    assert result == 0
    # init key with list by using regular rpush
    result = yield from redis.rpush(key, value1)
    assert result == 1

    result = yield from redis.rpushx(key, value2)
    assert result == 2

    # make sure that values actually inserted in right placed and order
    test_value = yield from redis.lrange(key, 0, -1)
    assert test_value == [value1, value2]

    with pytest.raises(TypeError):
        yield from redis.rpushx(None, value1)
