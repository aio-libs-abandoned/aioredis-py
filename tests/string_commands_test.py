import asyncio
import pytest

from aioredis import ReplyError


@asyncio.coroutine
def add(redis, key, value):
    ok = yield from redis.set(key, value)
    assert ok is True


@pytest.mark.run_loop
def test_append(redis):
    len_ = yield from redis.append('my-key', 'Hello')
    assert len_ == 5
    len_ = yield from redis.append('my-key', ', world!')
    assert len_ == 13

    val = yield from redis.connection.execute('GET', 'my-key')
    assert val == b'Hello, world!'

    with pytest.raises(TypeError):
        yield from redis.append(None, 'value')
    with pytest.raises(TypeError):
        yield from redis.append('none-key', None)


@pytest.mark.run_loop
def test_bitcount(redis):
    yield from add(redis, 'my-key', b'\x00\x10\x01')

    ret = yield from redis.bitcount('my-key')
    assert ret == 2
    ret = yield from redis.bitcount('my-key', 0, 0)
    assert ret == 0
    ret = yield from redis.bitcount('my-key', 1, 1)
    assert ret == 1
    ret = yield from redis.bitcount('my-key', 2, 2)
    assert ret == 1
    ret = yield from redis.bitcount('my-key', 0, 1)
    assert ret == 1
    ret = yield from redis.bitcount('my-key', 0, 2)
    assert ret == 2
    ret = yield from redis.bitcount('my-key', 1, 2)
    assert ret == 2
    ret = yield from redis.bitcount('my-key', 2, 3)
    assert ret == 1
    ret = yield from redis.bitcount('my-key', 0, -1)
    assert ret == 2

    with pytest.raises(TypeError):
        yield from redis.bitcount(None, 2, 2)
    with pytest.raises(TypeError):
        yield from redis.bitcount('my-key', None, 2)
    with pytest.raises(TypeError):
        yield from redis.bitcount('my-key', 2, None)


@pytest.mark.run_loop
def test_bitop_and(redis):
    key1, value1 = b'key:bitop:and:1', 5
    key2, value2 = b'key:bitop:and:2', 7

    yield from add(redis, key1, value1)
    yield from add(redis, key2, value2)

    destkey = b'key:bitop:dest'

    yield from redis.bitop_and(destkey, key1, key2)
    test_value = yield from redis.get(destkey)
    assert test_value == b'5'

    with pytest.raises(TypeError):
        yield from redis.bitop_and(None, key1, key2)
    with pytest.raises(TypeError):
        yield from redis.bitop_and(destkey, None)
    with pytest.raises(TypeError):
        yield from redis.bitop_and(destkey, key1, None)


@pytest.mark.run_loop
def test_bitop_or(redis):
    key1, value1 = b'key:bitop:or:1', 5
    key2, value2 = b'key:bitop:or:2', 7

    yield from add(redis, key1, value1)
    yield from add(redis, key2, value2)

    destkey = b'key:bitop:dest'

    yield from redis.bitop_or(destkey, key1, key2)
    test_value = yield from redis.get(destkey)
    assert test_value == b'7'

    with pytest.raises(TypeError):
        yield from redis.bitop_or(None, key1, key2)
    with pytest.raises(TypeError):
        yield from redis.bitop_or(destkey, None)
    with pytest.raises(TypeError):
        yield from redis.bitop_or(destkey, key1, None)


@pytest.mark.run_loop
def test_bitop_xor(redis):
    key1, value1 = b'key:bitop:xor:1', 5
    key2, value2 = b'key:bitop:xor:2', 7

    yield from add(redis, key1, value1)
    yield from add(redis, key2, value2)

    destkey = b'key:bitop:dest'

    yield from redis.bitop_xor(destkey, key1, key2)
    test_value = yield from redis.get(destkey)
    assert test_value == b'\x02'

    with pytest.raises(TypeError):
        yield from redis.bitop_xor(None, key1, key2)
    with pytest.raises(TypeError):
        yield from redis.bitop_xor(destkey, None)
    with pytest.raises(TypeError):
        yield from redis.bitop_xor(destkey, key1, None)


@pytest.mark.run_loop
def test_bitop_not(redis):
    key1, value1 = b'key:bitop:not:1', 5
    yield from add(redis, key1, value1)

    destkey = b'key:bitop:dest'

    yield from redis.bitop_not(destkey, key1)
    res = yield from redis.get(destkey)
    assert res == b'\xca'

    with pytest.raises(TypeError):
        yield from redis.bitop_not(None, key1)
    with pytest.raises(TypeError):
        yield from redis.bitop_not(destkey, None)


@pytest.redis_version(2, 8, 0, reason='BITPOS is available since redis>=2.8.0')
@pytest.mark.run_loop
def test_bitpos(redis):
    key, value = b'key:bitop', b'\xff\xf0\x00'
    yield from add(redis, key, value)
    test_value = yield from redis.bitpos(key, 0, end=3)
    assert test_value == 12

    test_value = yield from redis.bitpos(key, 0, 2, 3)
    assert test_value == 16

    key, value = b'key:bitop', b'\x00\xff\xf0'
    yield from add(redis, key, value)
    test_value = yield from redis.bitpos(key, 1, 0)
    assert test_value == 8

    test_value = yield from redis.bitpos(key, 1, 1)
    assert test_value == 8

    key, value = b'key:bitop', b'\x00\x00\x00'
    yield from add(redis, key, value)
    test_value = yield from redis.bitpos(key, 1, 0)
    assert test_value == -1

    test_value = yield from redis.bitpos(b'not:' + key, 1)
    assert test_value == -1

    with pytest.raises(TypeError):
        test_value = yield from redis.bitpos(None, 1)

    with pytest.raises(ValueError):
        test_value = yield from redis.bitpos(key, 7)


@pytest.mark.run_loop
def test_decr(redis):
    yield from redis.delete('key')

    res = yield from redis.decr('key')
    assert res == -1
    res = yield from redis.decr('key')
    assert res == -2

    with pytest.raises(ReplyError):
        yield from add(redis, 'key', 'val')
        yield from redis.decr('key')
    with pytest.raises(ReplyError):
        yield from add(redis, 'key', 1.0)
        yield from redis.decr('key')
    with pytest.raises(TypeError):
        yield from redis.decr(None)


@pytest.mark.run_loop
def test_decrby(redis):
    yield from redis.delete('key')

    res = yield from redis.decrby('key', 1)
    assert res == -1
    res = yield from redis.decrby('key', 10)
    assert res == -11
    res = yield from redis.decrby('key', -1)
    assert res == -10

    with pytest.raises(ReplyError):
        yield from add(redis, 'key', 'val')
        yield from redis.decrby('key', 1)
    with pytest.raises(ReplyError):
        yield from add(redis, 'key', 1.0)
        yield from redis.decrby('key', 1)
    with pytest.raises(TypeError):
        yield from redis.decrby(None, 1)
    with pytest.raises(TypeError):
        yield from redis.decrby('key', None)


@pytest.mark.run_loop
def test_get(redis):
    yield from add(redis, 'my-key', 'value')
    ret = yield from redis.get('my-key')
    assert ret == b'value'

    yield from add(redis, 'my-key', 123)
    ret = yield from redis.get('my-key')
    assert ret == b'123'

    ret = yield from redis.get('bad-key')
    assert ret is None

    with pytest.raises(TypeError):
        yield from redis.get(None)


@pytest.mark.run_loop
def test_getbit(redis):
    key, value = b'key:getbit', 10
    yield from add(redis, key, value)

    result = yield from redis.setbit(key, 7, 1)
    assert result == 1

    test_value = yield from redis.getbit(key, 0)
    assert test_value == 0

    test_value = yield from redis.getbit(key, 7)
    assert test_value == 1

    test_value = yield from redis.getbit(b'not:' + key, 7)
    assert test_value == 0

    test_value = yield from redis.getbit(key, 100)
    assert test_value == 0

    with pytest.raises(TypeError):
        yield from redis.getbit(None, 0)
    with pytest.raises(TypeError):
        yield from redis.getbit(key, b'one')
    with pytest.raises(ValueError):
        yield from redis.getbit(key, -7)


@pytest.mark.run_loop
def test_getrange(redis):
    key, value = b'key:getrange', b'This is a string'
    yield from add(redis, key, value)

    test_value = yield from redis.getrange(key, 0, 3)
    assert test_value == b'This'

    test_value = yield from redis.getrange(key, -3, -1)
    assert test_value == b'ing'

    test_value = yield from redis.getrange(key, 0, -1)
    assert test_value == b'This is a string'
    test_value = yield from redis.getrange(
        key, 0, -1, encoding='utf-8')
    assert test_value == 'This is a string'

    test_value = yield from redis.getrange(key, 10, 100)
    assert test_value == b'string'
    test_value = yield from redis.getrange(
        key, 10, 100, encoding='utf-8')
    assert test_value == 'string'

    test_value = yield from redis.getrange(key, 50, 100)
    assert test_value == b''

    with pytest.raises(TypeError):
        yield from redis.getrange(None, 0, 3)
    with pytest.raises(TypeError):
        yield from redis.getrange(key, b'one', 3)
    with pytest.raises(TypeError):
        yield from redis.getrange(key, 0, b'seven')


@pytest.mark.run_loop
def test_getset(redis):
    key, value = b'key:getset', b'hello'
    yield from add(redis, key, value)

    test_value = yield from redis.getset(key, b'asyncio')
    assert test_value == b'hello'

    test_value = yield from redis.get(key)
    assert test_value == b'asyncio'

    test_value = yield from redis.getset(
        key, 'world', encoding='utf-8')
    assert test_value == 'asyncio'

    test_value = yield from redis.getset(b'not:' + key, b'asyncio')
    assert test_value is None

    test_value = yield from redis.get(b'not:' + key)
    assert test_value == b'asyncio'

    with pytest.raises(TypeError):
        yield from redis.getset(None, b'asyncio')


@pytest.mark.run_loop
def test_incr(redis):
    yield from redis.delete('key')

    res = yield from redis.incr('key')
    assert res == 1
    res = yield from redis.incr('key')
    assert res == 2

    with pytest.raises(ReplyError):
        yield from add(redis, 'key', 'val')
        yield from redis.incr('key')
    with pytest.raises(ReplyError):
        yield from add(redis, 'key', 1.0)
        yield from redis.incr('key')
    with pytest.raises(TypeError):
        yield from redis.incr(None)


@pytest.mark.run_loop
def test_incrby(redis):
    yield from redis.delete('key')

    res = yield from redis.incrby('key', 1)
    assert res == 1
    res = yield from redis.incrby('key', 10)
    assert res == 11
    res = yield from redis.incrby('key', -1)
    assert res == 10

    with pytest.raises(ReplyError):
        yield from add(redis, 'key', 'val')
        yield from redis.incrby('key', 1)
    with pytest.raises(ReplyError):
        yield from add(redis, 'key', 1.0)
        yield from redis.incrby('key', 1)
    with pytest.raises(TypeError):
        yield from redis.incrby(None, 1)
    with pytest.raises(TypeError):
        yield from redis.incrby('key', None)


@pytest.mark.run_loop
def test_incrbyfloat(redis):
    yield from redis.delete('key')

    res = yield from redis.incrbyfloat('key', 1.0)
    assert res == 1.0
    res = yield from redis.incrbyfloat('key', 10.5)
    assert res == 11.5
    res = yield from redis.incrbyfloat('key', -1.0)
    assert res == 10.5
    yield from add(redis, 'key', 2)
    res = yield from redis.incrbyfloat('key', 0.5)
    assert res == 2.5

    with pytest.raises(ReplyError):
        yield from add(redis, 'key', 'val')
        yield from redis.incrbyfloat('key', 1.0)
    with pytest.raises(TypeError):
        yield from redis.incrbyfloat(None, 1.0)
    with pytest.raises(TypeError):
        yield from redis.incrbyfloat('key', None)
    with pytest.raises(TypeError):
        yield from redis.incrbyfloat('key', 1)
    with pytest.raises(TypeError):
        yield from redis.incrbyfloat('key', '1.0')


@pytest.mark.run_loop
def test_mget(redis):
    key1, value1 = b'foo', b'bar'
    key2, value2 = b'baz', b'bzz'
    yield from add(redis, key1, value1)
    yield from add(redis, key2, value2)

    res = yield from redis.mget('key')
    assert res == [None]
    res = yield from redis.mget('key', 'key')
    assert res == [None, None]

    res = yield from redis.mget(key1, key2)
    assert res == [value1, value2]

    # test encoding param
    res = yield from redis.mget(key1, key2, encoding='utf-8')
    assert res == ['bar', 'bzz']

    with pytest.raises(TypeError):
        yield from redis.mget(None, key2)
    with pytest.raises(TypeError):
        yield from redis.mget(key1, None)


@pytest.mark.run_loop
def test_mset(redis):
    key1, value1 = b'key:mset:1', b'hello'
    key2, value2 = b'key:mset:2', b'world'

    yield from redis.mset(key1, value1, key2, value2)

    test_value = yield from redis.mget(key1, key2)
    assert test_value == [value1, value2]

    yield from redis.mset(b'other:' + key1, b'other:' + value1)
    test_value = yield from redis.get(b'other:' + key1)
    assert test_value == b'other:' + value1

    with pytest.raises(TypeError):
        yield from redis.mset(None, value1)
    with pytest.raises(TypeError):
        yield from redis.mset(key1, value1, key1)


@pytest.mark.run_loop
def test_msetnx(redis):
    key1, value1 = b'key:msetnx:1', b'Hello'
    key2, value2 = b'key:msetnx:2', b'there'
    key3, value3 = b'key:msetnx:3', b'world'

    res = yield from redis.msetnx(key1, value1, key2, value2)
    assert res == 1
    res = yield from redis.mget(key1, key2)
    assert res == [value1, value2]
    res = yield from redis.msetnx(key2, value2, key3, value3)
    assert res == 0
    res = yield from redis.mget(key1, key2, key3)
    assert res == [value1, value2, None]

    with pytest.raises(TypeError):
        yield from redis.msetnx(None, value1)
    with pytest.raises(TypeError):
        yield from redis.msetnx(key1, value1, key2)


@pytest.mark.run_loop
def test_psetex(redis, loop):
    key, value = b'key:psetex:1', b'Hello'
    # test expiration in milliseconds
    tr = redis.multi_exec()
    fut1 = tr.psetex(key, 10, value)
    fut2 = tr.get(key)
    yield from tr.execute()
    yield from fut1
    test_value = yield from fut2
    assert test_value == value

    yield from asyncio.sleep(0.050, loop=loop)
    test_value = yield from redis.get(key)
    assert test_value is None

    with pytest.raises(TypeError):
        yield from redis.psetex(None, 10, value)
    with pytest.raises(TypeError):
        yield from redis.psetex(key, 7.5, value)


@pytest.mark.run_loop
def test_set(redis):
    ok = yield from redis.set('my-key', 'value')
    assert ok is True

    with pytest.raises(TypeError):
        yield from redis.set(None, 'value')


@pytest.mark.run_loop
def test_set_expire(redis, loop):
    key, value = b'key:set:expire', b'foo'
    # test expiration in milliseconds
    tr = redis.multi_exec()
    fut1 = tr.set(key, value, pexpire=10)
    fut2 = tr.get(key)
    yield from tr.execute()
    yield from fut1
    result_1 = yield from fut2
    assert result_1 == value
    yield from asyncio.sleep(0.050, loop=loop)
    result_2 = yield from redis.get(key)
    assert result_2 is None

    # same thing but timeout in seconds
    tr = redis.multi_exec()
    fut1 = tr.set(key, value, expire=1)
    fut2 = tr.get(key)
    yield from tr.execute()
    yield from fut1
    result_3 = yield from fut2
    assert result_3 == value
    yield from asyncio.sleep(1.050, loop=loop)
    result_4 = yield from redis.get(key)
    assert result_4 is None


@pytest.mark.run_loop
def test_set_only_if_not_exists(redis):
    key, value = b'key:set:only_if_not_exists', b'foo'
    yield from redis.set(
        key, value, exist=redis.SET_IF_NOT_EXIST)
    result_1 = yield from redis.get(key)
    assert result_1 == value

    # new values not set cos, values exists
    yield from redis.set(
        key, "foo2", exist=redis.SET_IF_NOT_EXIST)
    result_2 = yield from redis.get(key)
    # nothing changed result is same "foo"
    assert result_2 == value


@pytest.mark.run_loop
def test_set_only_if_exists(redis):
    key, value = b'key:set:only_if_exists', b'only_if_exists:foo'
    # ensure that such key does not exits, and value not sets
    yield from redis.delete(key)
    yield from redis.set(key, value, exist=redis.SET_IF_EXIST)
    result_1 = yield from redis.get(key)
    assert result_1 is None

    # ensure key exits, and value updates
    yield from redis.set(key, value)
    yield from redis.set(key, b'foo', exist=redis.SET_IF_EXIST)
    result_2 = yield from redis.get(key)
    assert result_2 == b'foo'


@pytest.mark.run_loop
def test_set_wrong_input(redis):
    key, value = b'key:set:', b'foo'

    with pytest.raises(TypeError):
        yield from redis.set(None, value)
    with pytest.raises(TypeError):
        yield from redis.set(key, value, expire=7.8)
    with pytest.raises(TypeError):
        yield from redis.set(key, value, pexpire=7.8)


@pytest.mark.run_loop
def test_setbit(redis):
    key = b'key:setbit'
    result = yield from redis.setbit(key, 7, 1)
    assert result == 0
    test_value = yield from redis.getbit(key, 7)
    assert test_value == 1

    with pytest.raises(TypeError):
        yield from redis.setbit(None, 7, 1)
    with pytest.raises(TypeError):
        yield from redis.setbit(key, 7.5, 1)
    with pytest.raises(ValueError):
        yield from redis.setbit(key, -1, 1)
    with pytest.raises(ValueError):
        yield from redis.setbit(key, 1, 7)


@pytest.mark.run_loop
def test_setex(redis, loop):
    key, value = b'key:setex:1', b'Hello'
    tr = redis.multi_exec()
    fut1 = tr.setex(key, 1, value)
    fut2 = tr.get(key)
    yield from tr.execute()
    yield from fut1
    test_value = yield from fut2
    assert test_value == value
    yield from asyncio.sleep(1.050, loop=loop)
    test_value = yield from redis.get(key)
    assert test_value is None

    tr = redis.multi_exec()
    fut1 = tr.setex(key, 0.1, value)
    fut2 = tr.get(key)
    yield from tr.execute()
    yield from fut1
    test_value = yield from fut2
    assert test_value == value
    yield from asyncio.sleep(0.50, loop=loop)
    test_value = yield from redis.get(key)
    assert test_value is None

    with pytest.raises(TypeError):
        yield from redis.setex(None, 1, value)
    with pytest.raises(TypeError):
        yield from redis.setex(key, b'one', value)


@pytest.mark.run_loop
def test_setnx(redis):
    key, value = b'key:setnx:1', b'Hello'
    # set fresh new value
    test_value = yield from redis.setnx(key, value)
    # 1 means value has been set
    assert test_value == 1
    # fetch installed value just to be sure
    test_value = yield from redis.get(key)
    assert test_value == value
    # try to set new value on same key
    test_value = yield from redis.setnx(key, b'other:' + value)
    # 0 means value has not been set
    assert test_value == 0
    # make sure that value was not changed
    test_value = yield from redis.get(key)
    assert test_value == value

    with pytest.raises(TypeError):
        yield from redis.setnx(None, value)


@pytest.mark.run_loop
def test_setrange(redis):
    key, value = b'key:setrange', b'Hello World'
    yield from add(redis, key, value)
    test_value = yield from redis.setrange(key, 6, b'Redis')
    assert test_value == 11
    test_value = yield from redis.get(key)
    assert test_value == b'Hello Redis'

    test_value = yield from redis.setrange(b'not:' + key, 6, b'Redis')
    assert test_value == 11
    test_value = yield from redis.get(b'not:' + key)
    assert test_value == b'\x00\x00\x00\x00\x00\x00Redis'

    with pytest.raises(TypeError):
        yield from redis.setrange(None, 6, b'Redis')
    with pytest.raises(TypeError):
        yield from redis.setrange(key, 0.7, b'Redis')
    with pytest.raises(ValueError):
        yield from redis.setrange(key, -1, b'Redis')


@pytest.mark.run_loop
def test_strlen(redis):
    key, value = b'key:strlen', b'asyncio'
    yield from add(redis, key, value)
    test_value = yield from redis.strlen(key)
    assert test_value == len(value)

    test_value = yield from redis.strlen(b'not:' + key)
    assert test_value == 0

    with pytest.raises(TypeError):
        yield from redis.strlen(None)


@pytest.mark.run_loop
def test_cancel_hang(redis):
    exists_coro = redis.execute("EXISTS", b"key:test1")
    exists_coro.cancel()
    exists_check = yield from redis.exists(b"key:test2")
    assert not exists_check


@pytest.mark.run_loop
def test_set_enc(create_redis, loop, server):
    redis = yield from create_redis(
        server.tcp_address, loop=loop, encoding='utf-8')
    TEST_KEY = 'my-key'
    ok = yield from redis.set(TEST_KEY, 'value')
    assert ok is True

    with pytest.raises(TypeError):
        yield from redis.set(None, 'value')

    yield from redis.delete(TEST_KEY)
