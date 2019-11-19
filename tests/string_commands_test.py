import asyncio
import pytest

from aioredis import ReplyError
from _testutils import redis_version


async def add(redis, key, value):
    ok = await redis.set(key, value)
    assert ok is True


async def test_append(redis):
    len_ = await redis.append('my-key', 'Hello')
    assert len_ == 5
    len_ = await redis.append('my-key', ', world!')
    assert len_ == 13

    val = await redis.connection.execute('GET', 'my-key')
    assert val == b'Hello, world!'

    with pytest.raises(TypeError):
        await redis.append(None, 'value')
    with pytest.raises(TypeError):
        await redis.append('none-key', None)


async def test_bitcount(redis):
    await add(redis, 'my-key', b'\x00\x10\x01')

    ret = await redis.bitcount('my-key')
    assert ret == 2
    ret = await redis.bitcount('my-key', 0, 0)
    assert ret == 0
    ret = await redis.bitcount('my-key', 1, 1)
    assert ret == 1
    ret = await redis.bitcount('my-key', 2, 2)
    assert ret == 1
    ret = await redis.bitcount('my-key', 0, 1)
    assert ret == 1
    ret = await redis.bitcount('my-key', 0, 2)
    assert ret == 2
    ret = await redis.bitcount('my-key', 1, 2)
    assert ret == 2
    ret = await redis.bitcount('my-key', 2, 3)
    assert ret == 1
    ret = await redis.bitcount('my-key', 0, -1)
    assert ret == 2

    with pytest.raises(TypeError):
        await redis.bitcount(None, 2, 2)
    with pytest.raises(TypeError):
        await redis.bitcount('my-key', None, 2)
    with pytest.raises(TypeError):
        await redis.bitcount('my-key', 2, None)


async def test_bitop_and(redis):
    key1, value1 = b'key:bitop:and:1', 5
    key2, value2 = b'key:bitop:and:2', 7

    await add(redis, key1, value1)
    await add(redis, key2, value2)

    destkey = b'key:bitop:dest'

    await redis.bitop_and(destkey, key1, key2)
    test_value = await redis.get(destkey)
    assert test_value == b'5'

    with pytest.raises(TypeError):
        await redis.bitop_and(None, key1, key2)
    with pytest.raises(TypeError):
        await redis.bitop_and(destkey, None)
    with pytest.raises(TypeError):
        await redis.bitop_and(destkey, key1, None)


async def test_bitop_or(redis):
    key1, value1 = b'key:bitop:or:1', 5
    key2, value2 = b'key:bitop:or:2', 7

    await add(redis, key1, value1)
    await add(redis, key2, value2)

    destkey = b'key:bitop:dest'

    await redis.bitop_or(destkey, key1, key2)
    test_value = await redis.get(destkey)
    assert test_value == b'7'

    with pytest.raises(TypeError):
        await redis.bitop_or(None, key1, key2)
    with pytest.raises(TypeError):
        await redis.bitop_or(destkey, None)
    with pytest.raises(TypeError):
        await redis.bitop_or(destkey, key1, None)


async def test_bitop_xor(redis):
    key1, value1 = b'key:bitop:xor:1', 5
    key2, value2 = b'key:bitop:xor:2', 7

    await add(redis, key1, value1)
    await add(redis, key2, value2)

    destkey = b'key:bitop:dest'

    await redis.bitop_xor(destkey, key1, key2)
    test_value = await redis.get(destkey)
    assert test_value == b'\x02'

    with pytest.raises(TypeError):
        await redis.bitop_xor(None, key1, key2)
    with pytest.raises(TypeError):
        await redis.bitop_xor(destkey, None)
    with pytest.raises(TypeError):
        await redis.bitop_xor(destkey, key1, None)


async def test_bitop_not(redis):
    key1, value1 = b'key:bitop:not:1', 5
    await add(redis, key1, value1)

    destkey = b'key:bitop:dest'

    await redis.bitop_not(destkey, key1)
    res = await redis.get(destkey)
    assert res == b'\xca'

    with pytest.raises(TypeError):
        await redis.bitop_not(None, key1)
    with pytest.raises(TypeError):
        await redis.bitop_not(destkey, None)


@redis_version(2, 8, 0, reason='BITPOS is available since redis>=2.8.0')
async def test_bitpos(redis):
    key, value = b'key:bitop', b'\xff\xf0\x00'
    await add(redis, key, value)
    test_value = await redis.bitpos(key, 0, end=3)
    assert test_value == 12

    test_value = await redis.bitpos(key, 0, 2, 3)
    assert test_value == 16

    key, value = b'key:bitop', b'\x00\xff\xf0'
    await add(redis, key, value)
    test_value = await redis.bitpos(key, 1, 0)
    assert test_value == 8

    test_value = await redis.bitpos(key, 1, 1)
    assert test_value == 8

    key, value = b'key:bitop', b'\x00\x00\x00'
    await add(redis, key, value)
    test_value = await redis.bitpos(key, 1, 0)
    assert test_value == -1

    test_value = await redis.bitpos(b'not:' + key, 1)
    assert test_value == -1

    with pytest.raises(TypeError):
        test_value = await redis.bitpos(None, 1)

    with pytest.raises(ValueError):
        test_value = await redis.bitpos(key, 7)


async def test_decr(redis):
    await redis.delete('key')

    res = await redis.decr('key')
    assert res == -1
    res = await redis.decr('key')
    assert res == -2

    with pytest.raises(ReplyError):
        await add(redis, 'key', 'val')
        await redis.decr('key')
    with pytest.raises(ReplyError):
        await add(redis, 'key', 1.0)
        await redis.decr('key')
    with pytest.raises(TypeError):
        await redis.decr(None)


async def test_decrby(redis):
    await redis.delete('key')

    res = await redis.decrby('key', 1)
    assert res == -1
    res = await redis.decrby('key', 10)
    assert res == -11
    res = await redis.decrby('key', -1)
    assert res == -10

    with pytest.raises(ReplyError):
        await add(redis, 'key', 'val')
        await redis.decrby('key', 1)
    with pytest.raises(ReplyError):
        await add(redis, 'key', 1.0)
        await redis.decrby('key', 1)
    with pytest.raises(TypeError):
        await redis.decrby(None, 1)
    with pytest.raises(TypeError):
        await redis.decrby('key', None)


async def test_get(redis):
    await add(redis, 'my-key', 'value')
    ret = await redis.get('my-key')
    assert ret == b'value'

    await add(redis, 'my-key', 123)
    ret = await redis.get('my-key')
    assert ret == b'123'

    ret = await redis.get('bad-key')
    assert ret is None

    with pytest.raises(TypeError):
        await redis.get(None)


async def test_getbit(redis):
    key, value = b'key:getbit', 10
    await add(redis, key, value)

    result = await redis.setbit(key, 7, 1)
    assert result == 1

    test_value = await redis.getbit(key, 0)
    assert test_value == 0

    test_value = await redis.getbit(key, 7)
    assert test_value == 1

    test_value = await redis.getbit(b'not:' + key, 7)
    assert test_value == 0

    test_value = await redis.getbit(key, 100)
    assert test_value == 0

    with pytest.raises(TypeError):
        await redis.getbit(None, 0)
    with pytest.raises(TypeError):
        await redis.getbit(key, b'one')
    with pytest.raises(ValueError):
        await redis.getbit(key, -7)


async def test_getrange(redis):
    key, value = b'key:getrange', b'This is a string'
    await add(redis, key, value)

    test_value = await redis.getrange(key, 0, 3)
    assert test_value == b'This'

    test_value = await redis.getrange(key, -3, -1)
    assert test_value == b'ing'

    test_value = await redis.getrange(key, 0, -1)
    assert test_value == b'This is a string'
    test_value = await redis.getrange(
        key, 0, -1, encoding='utf-8')
    assert test_value == 'This is a string'

    test_value = await redis.getrange(key, 10, 100)
    assert test_value == b'string'
    test_value = await redis.getrange(
        key, 10, 100, encoding='utf-8')
    assert test_value == 'string'

    test_value = await redis.getrange(key, 50, 100)
    assert test_value == b''

    with pytest.raises(TypeError):
        await redis.getrange(None, 0, 3)
    with pytest.raises(TypeError):
        await redis.getrange(key, b'one', 3)
    with pytest.raises(TypeError):
        await redis.getrange(key, 0, b'seven')


async def test_getset(redis):
    key, value = b'key:getset', b'hello'
    await add(redis, key, value)

    test_value = await redis.getset(key, b'asyncio')
    assert test_value == b'hello'

    test_value = await redis.get(key)
    assert test_value == b'asyncio'

    test_value = await redis.getset(
        key, 'world', encoding='utf-8')
    assert test_value == 'asyncio'

    test_value = await redis.getset(b'not:' + key, b'asyncio')
    assert test_value is None

    test_value = await redis.get(b'not:' + key)
    assert test_value == b'asyncio'

    with pytest.raises(TypeError):
        await redis.getset(None, b'asyncio')


async def test_incr(redis):
    await redis.delete('key')

    res = await redis.incr('key')
    assert res == 1
    res = await redis.incr('key')
    assert res == 2

    with pytest.raises(ReplyError):
        await add(redis, 'key', 'val')
        await redis.incr('key')
    with pytest.raises(ReplyError):
        await add(redis, 'key', 1.0)
        await redis.incr('key')
    with pytest.raises(TypeError):
        await redis.incr(None)


async def test_incrby(redis):
    await redis.delete('key')

    res = await redis.incrby('key', 1)
    assert res == 1
    res = await redis.incrby('key', 10)
    assert res == 11
    res = await redis.incrby('key', -1)
    assert res == 10

    with pytest.raises(ReplyError):
        await add(redis, 'key', 'val')
        await redis.incrby('key', 1)
    with pytest.raises(ReplyError):
        await add(redis, 'key', 1.0)
        await redis.incrby('key', 1)
    with pytest.raises(TypeError):
        await redis.incrby(None, 1)
    with pytest.raises(TypeError):
        await redis.incrby('key', None)


async def test_incrbyfloat(redis):
    await redis.delete('key')

    res = await redis.incrbyfloat('key', 1.0)
    assert res == 1.0
    res = await redis.incrbyfloat('key', 10.5)
    assert res == 11.5
    res = await redis.incrbyfloat('key', -1.0)
    assert res == 10.5
    await add(redis, 'key', 2)
    res = await redis.incrbyfloat('key', 0.5)
    assert res == 2.5

    with pytest.raises(ReplyError):
        await add(redis, 'key', 'val')
        await redis.incrbyfloat('key', 1.0)
    with pytest.raises(TypeError):
        await redis.incrbyfloat(None, 1.0)
    with pytest.raises(TypeError):
        await redis.incrbyfloat('key', None)
    with pytest.raises(TypeError):
        await redis.incrbyfloat('key', 1)
    with pytest.raises(TypeError):
        await redis.incrbyfloat('key', '1.0')


async def test_mget(redis):
    key1, value1 = b'foo', b'bar'
    key2, value2 = b'baz', b'bzz'
    await add(redis, key1, value1)
    await add(redis, key2, value2)

    res = await redis.mget('key')
    assert res == [None]
    res = await redis.mget('key', 'key')
    assert res == [None, None]

    res = await redis.mget(key1, key2)
    assert res == [value1, value2]

    # test encoding param
    res = await redis.mget(key1, key2, encoding='utf-8')
    assert res == ['bar', 'bzz']

    with pytest.raises(TypeError):
        await redis.mget(None, key2)
    with pytest.raises(TypeError):
        await redis.mget(key1, None)


async def test_mset(redis):
    key1, value1 = b'key:mset:1', b'hello'
    key2, value2 = b'key:mset:2', b'world'

    await redis.mset(key1, value1, key2, value2)

    test_value = await redis.mget(key1, key2)
    assert test_value == [value1, value2]

    await redis.mset(b'other:' + key1, b'other:' + value1)
    test_value = await redis.get(b'other:' + key1)
    assert test_value == b'other:' + value1

    with pytest.raises(TypeError):
        await redis.mset(None, value1)
    with pytest.raises(TypeError):
        await redis.mset(key1, value1, key1)


async def test_mset_with_dict(redis):
    array = [str(n) for n in range(10)]
    _dict = dict.fromkeys(array, 'default value', )

    await redis.mset(_dict)

    test_values = await redis.mget(*_dict.keys())
    assert test_values == [str.encode(val) for val in _dict.values()]

    with pytest.raises(TypeError):
        await redis.mset('param', )


async def test_msetnx(redis):
    key1, value1 = b'key:msetnx:1', b'Hello'
    key2, value2 = b'key:msetnx:2', b'there'
    key3, value3 = b'key:msetnx:3', b'world'

    res = await redis.msetnx(key1, value1, key2, value2)
    assert res == 1
    res = await redis.mget(key1, key2)
    assert res == [value1, value2]
    res = await redis.msetnx(key2, value2, key3, value3)
    assert res == 0
    res = await redis.mget(key1, key2, key3)
    assert res == [value1, value2, None]

    with pytest.raises(TypeError):
        await redis.msetnx(None, value1)
    with pytest.raises(TypeError):
        await redis.msetnx(key1, value1, key2)


async def test_psetex(redis):
    key, value = b'key:psetex:1', b'Hello'
    # test expiration in milliseconds
    tr = redis.multi_exec()
    fut1 = tr.psetex(key, 10, value)
    fut2 = tr.get(key)
    await tr.execute()
    await fut1
    test_value = await fut2
    assert test_value == value

    await asyncio.sleep(0.050)
    test_value = await redis.get(key)
    assert test_value is None

    with pytest.raises(TypeError):
        await redis.psetex(None, 10, value)
    with pytest.raises(TypeError):
        await redis.psetex(key, 7.5, value)


async def test_set(redis):
    ok = await redis.set('my-key', 'value')
    assert ok is True

    ok = await redis.set(b'my-key', b'value')
    assert ok is True

    ok = await redis.set(bytearray(b'my-key'), bytearray(b'value'))
    assert ok is True

    with pytest.raises(TypeError):
        await redis.set(None, 'value')


async def test_set_expire(redis):
    key, value = b'key:set:expire', b'foo'
    # test expiration in milliseconds
    tr = redis.multi_exec()
    fut1 = tr.set(key, value, pexpire=10)
    fut2 = tr.get(key)
    await tr.execute()
    await fut1
    result_1 = await fut2
    assert result_1 == value
    await asyncio.sleep(0.050)
    result_2 = await redis.get(key)
    assert result_2 is None

    # same thing but timeout in seconds
    tr = redis.multi_exec()
    fut1 = tr.set(key, value, expire=1)
    fut2 = tr.get(key)
    await tr.execute()
    await fut1
    result_3 = await fut2
    assert result_3 == value
    await asyncio.sleep(1.050)
    result_4 = await redis.get(key)
    assert result_4 is None


async def test_set_only_if_not_exists(redis):
    key, value = b'key:set:only_if_not_exists', b'foo'
    await redis.set(
        key, value, exist=redis.SET_IF_NOT_EXIST)
    result_1 = await redis.get(key)
    assert result_1 == value

    # new values not set cos, values exists
    await redis.set(
        key, "foo2", exist=redis.SET_IF_NOT_EXIST)
    result_2 = await redis.get(key)
    # nothing changed result is same "foo"
    assert result_2 == value


async def test_set_only_if_exists(redis):
    key, value = b'key:set:only_if_exists', b'only_if_exists:foo'
    # ensure that such key does not exits, and value not sets
    await redis.delete(key)
    await redis.set(key, value, exist=redis.SET_IF_EXIST)
    result_1 = await redis.get(key)
    assert result_1 is None

    # ensure key exits, and value updates
    await redis.set(key, value)
    await redis.set(key, b'foo', exist=redis.SET_IF_EXIST)
    result_2 = await redis.get(key)
    assert result_2 == b'foo'


async def test_set_wrong_input(redis):
    key, value = b'key:set:', b'foo'

    with pytest.raises(TypeError):
        await redis.set(None, value)
    with pytest.raises(TypeError):
        await redis.set(key, value, expire=7.8)
    with pytest.raises(TypeError):
        await redis.set(key, value, pexpire=7.8)


async def test_setbit(redis):
    key = b'key:setbit'
    result = await redis.setbit(key, 7, 1)
    assert result == 0
    test_value = await redis.getbit(key, 7)
    assert test_value == 1

    with pytest.raises(TypeError):
        await redis.setbit(None, 7, 1)
    with pytest.raises(TypeError):
        await redis.setbit(key, 7.5, 1)
    with pytest.raises(ValueError):
        await redis.setbit(key, -1, 1)
    with pytest.raises(ValueError):
        await redis.setbit(key, 1, 7)


async def test_setex(redis):
    key, value = b'key:setex:1', b'Hello'
    tr = redis.multi_exec()
    fut1 = tr.setex(key, 1, value)
    fut2 = tr.get(key)
    await tr.execute()
    await fut1
    test_value = await fut2
    assert test_value == value
    await asyncio.sleep(1.050)
    test_value = await redis.get(key)
    assert test_value is None

    tr = redis.multi_exec()
    fut1 = tr.setex(key, 0.1, value)
    fut2 = tr.get(key)
    await tr.execute()
    await fut1
    test_value = await fut2
    assert test_value == value
    await asyncio.sleep(0.50)
    test_value = await redis.get(key)
    assert test_value is None

    with pytest.raises(TypeError):
        await redis.setex(None, 1, value)
    with pytest.raises(TypeError):
        await redis.setex(key, b'one', value)


async def test_setnx(redis):
    key, value = b'key:setnx:1', b'Hello'
    # set fresh new value
    test_value = await redis.setnx(key, value)
    # 1 means value has been set
    assert test_value == 1
    # fetch installed value just to be sure
    test_value = await redis.get(key)
    assert test_value == value
    # try to set new value on same key
    test_value = await redis.setnx(key, b'other:' + value)
    # 0 means value has not been set
    assert test_value == 0
    # make sure that value was not changed
    test_value = await redis.get(key)
    assert test_value == value

    with pytest.raises(TypeError):
        await redis.setnx(None, value)


async def test_setrange(redis):
    key, value = b'key:setrange', b'Hello World'
    await add(redis, key, value)
    test_value = await redis.setrange(key, 6, b'Redis')
    assert test_value == 11
    test_value = await redis.get(key)
    assert test_value == b'Hello Redis'

    test_value = await redis.setrange(b'not:' + key, 6, b'Redis')
    assert test_value == 11
    test_value = await redis.get(b'not:' + key)
    assert test_value == b'\x00\x00\x00\x00\x00\x00Redis'

    with pytest.raises(TypeError):
        await redis.setrange(None, 6, b'Redis')
    with pytest.raises(TypeError):
        await redis.setrange(key, 0.7, b'Redis')
    with pytest.raises(ValueError):
        await redis.setrange(key, -1, b'Redis')


async def test_strlen(redis):
    key, value = b'key:strlen', b'asyncio'
    await add(redis, key, value)
    test_value = await redis.strlen(key)
    assert test_value == len(value)

    test_value = await redis.strlen(b'not:' + key)
    assert test_value == 0

    with pytest.raises(TypeError):
        await redis.strlen(None)


async def test_cancel_hang(redis):
    exists_coro = redis.execute("EXISTS", b"key:test1")
    exists_coro.cancel()
    exists_check = await redis.exists(b"key:test2")
    assert not exists_check


async def test_set_enc(create_redis, server):
    redis = await create_redis(server.tcp_address, encoding='utf-8')
    TEST_KEY = 'my-key'
    ok = await redis.set(TEST_KEY, 'value')
    assert ok is True

    with pytest.raises(TypeError):
        await redis.set(None, 'value')

    await redis.delete(TEST_KEY)
