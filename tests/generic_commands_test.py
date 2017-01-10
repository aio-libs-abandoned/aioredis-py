import asyncio
import time
import math
import pytest
from unittest import mock

from aioredis import ReplyError


@asyncio.coroutine
def add(redis, key, value):
    ok = yield from redis.connection.execute('set', key, value)
    assert ok == b'OK'


@pytest.mark.run_loop
def test_delete(redis):
    yield from add(redis, 'my-key', 123)
    yield from add(redis, 'other-key', 123)

    res = yield from redis.delete('my-key', 'non-existent-key')
    assert res == 1

    res = yield from redis.delete('other-key', 'other-key')
    assert res == 1

    with pytest.raises(TypeError):
        yield from redis.delete(None)

    with pytest.raises(TypeError):
        yield from redis.delete('my-key', 'my-key', None)


@pytest.mark.run_loop
def test_dump(redis):
    yield from add(redis, 'my-key', 123)

    data = yield from redis.dump('my-key')
    assert data == mock.ANY
    assert isinstance(data, (bytes, bytearray))
    assert len(data) > 0

    data = yield from redis.dump('non-existent-key')
    assert data is None

    with pytest.raises(TypeError):
        yield from redis.dump(None)


@pytest.mark.run_loop
def test_exists(redis, server):
    yield from add(redis, 'my-key', 123)

    res = yield from redis.exists('my-key')
    assert isinstance(res, int)
    assert res == 1

    res = yield from redis.exists('non-existent-key')
    assert isinstance(res, int)
    assert res == 0

    with pytest.raises(TypeError):
        yield from redis.exists(None)
    if server.version < (3, 0, 3):
        with pytest.raises(ReplyError):
            yield from redis.exists('key-1', 'key-2')


@pytest.redis_version(
    3, 0, 3, reason='Multi-key EXISTS available since redis>=2.8.0')
@pytest.mark.run_loop
def test_exists_multiple(redis):
    yield from add(redis, 'my-key', 123)

    res = yield from redis.exists('my-key', 'other-key')
    assert isinstance(res, int)
    assert res == 1

    res = yield from redis.exists('my-key', 'my-key')
    assert isinstance(res, int)
    assert res == 2

    res = yield from redis.exists('foo', 'bar')
    assert isinstance(res, int)
    assert res == 0


@pytest.mark.run_loop
def test_expire(redis):
    yield from add(redis, 'my-key', 132)

    res = yield from redis.expire('my-key', 10)
    assert res is True

    res = yield from redis.connection.execute('TTL', 'my-key')
    assert res >= 10

    yield from redis.expire('my-key', -1)
    res = yield from redis.exists('my-key')
    assert not res

    res = yield from redis.expire('other-key', 1000)
    assert res is False

    yield from add(redis, 'my-key', 1)
    res = yield from redis.expire('my-key', 10.0)
    assert res is True
    res = yield from redis.connection.execute('TTL', 'my-key')
    assert res >= 10

    with pytest.raises(TypeError):
        yield from redis.expire(None, 123)
    with pytest.raises(TypeError):
        yield from redis.expire('my-key', 'timeout')

# @pytest.mark.run_loop
# def test_wait_expire():
#     return
#     yield from .add('my-key', 123)
#     res = yield from .redis.expire('my-key', 1)
#     .assertIs(res, True)

#     yield from asyncio.sleep(1, loop=.loop)

#     res = yield from .redis.exists('my-key')
#     .assertIs(res, False)

# @pytest.mark.run_loop
# def test_wait_expireat():
#     return
#     yield from .add('my-key', 123)
#     ts = int(time.time() + 1)
#     res = yield from .redis.expireat('my-key', ts)

#     yield from asyncio.sleep(ts - time.time(), loop=.loop)
#     res = yield from .redis.exists('my-key')
#     .assertIs(res, False)


@pytest.mark.run_loop
def test_expireat(redis):
    yield from add(redis, 'my-key', 123)
    now = math.ceil(time.time())

    res = yield from redis.expireat('my-key', now + 10)
    assert res is True

    res = yield from redis.connection.execute('TTL', 'my-key')
    assert res >= 10

    res = yield from redis.expireat('my-key', -1)
    assert res is True

    res = yield from redis.exists('my-key')
    assert not res

    yield from add(redis, 'my-key', 123)

    res = yield from redis.expireat('my-key', 0)
    assert res is True

    res = yield from redis.exists('my-key')
    assert not res

    yield from add(redis, 'my-key', 123)
    res = yield from redis.expireat('my-key', time.time() + 10)
    assert res is True

    res = yield from redis.connection.execute('TTL', 'my-key')
    assert res >= 10

    yield from add(redis, 'my-key', 123)
    with pytest.raises(TypeError):
        yield from redis.expireat(None, 123)
    with pytest.raises(TypeError):
        yield from redis.expireat('my-key', 'timestamp')


@pytest.mark.run_loop
def test_keys(redis):
    res = yield from redis.keys('*pattern*')
    assert res == []

    yield from redis.connection.execute('FLUSHDB')
    res = yield from redis.keys('*')
    assert res == []

    yield from add(redis, 'my-key-1', 1)
    yield from add(redis, 'my-key-ab', 1)

    res = yield from redis.keys('my-key-?')
    assert res == [b'my-key-1']
    res = yield from redis.keys('my-key-*')
    assert sorted(res) == [b'my-key-1', b'my-key-ab']

    # test with encoding param
    res = yield from redis.keys('my-key-*', encoding='utf-8')
    assert sorted(res) == ['my-key-1', 'my-key-ab']

    with pytest.raises(TypeError):
        yield from redis.keys(None)


@pytest.mark.run_loop
def test_migrate(redis, create_redis, loop, serverB):
    yield from add(redis, 'my-key', 123)

    conn2 = yield from create_redis(serverB.tcp_address,
                                    db=2, loop=loop)
    yield from conn2.delete('my-key')
    assert (yield from redis.exists('my-key'))
    assert not (yield from conn2.exists('my-key'))

    ok = yield from redis.migrate(
        'localhost', serverB.tcp_address.port, 'my-key', 2, 1000)
    assert ok is True
    assert not (yield from redis.exists('my-key'))
    assert (yield from conn2.exists('my-key'))

    with pytest.raises_regex(TypeError, "host .* str"):
        yield from redis.migrate(None, 1234, 'key', 1, 23)
    with pytest.raises_regex(TypeError, "args .* None"):
        yield from redis.migrate('host', '1234',  None, 1, 123)
    with pytest.raises_regex(TypeError, "dest_db .* int"):
        yield from redis.migrate('host', 123, 'key', 1.0, 123)
    with pytest.raises_regex(TypeError, "timeout .* int"):
        yield from redis.migrate('host', '1234', 'key', 2, None)
    with pytest.raises_regex(ValueError, "Got empty host"):
        yield from redis.migrate('', '123', 'key', 1, 123)
    with pytest.raises_regex(ValueError, "dest_db .* greater equal 0"):
        yield from redis.migrate('host', 6379, 'key', -1, 1000)
    with pytest.raises_regex(ValueError, "timeout .* greater equal 0"):
        yield from redis.migrate('host', 6379, 'key', 1, -1000)


@pytest.mark.run_loop
def test_move(redis):
    yield from add(redis, 'my-key', 123)

    assert redis.db == 0
    res = yield from redis.move('my-key', 1)
    assert res is True

    with pytest.raises(TypeError):
        yield from redis.move(None, 1)
    with pytest.raises(TypeError):
        yield from redis.move('my-key', None)
    with pytest.raises(ValueError):
        yield from redis.move('my-key', -1)
    with pytest.raises(TypeError):
        yield from redis.move('my-key', 'not db')


@pytest.mark.run_loop
def test_object_refcount(redis):
    yield from add(redis, 'foo', 'bar')

    res = yield from redis.object_refcount('foo')
    assert res == 1
    res = yield from redis.object_refcount('non-existent-key')
    assert res is None

    with pytest.raises(TypeError):
        yield from redis.object_refcount(None)


@pytest.mark.run_loop
def test_object_encoding(redis, server):
    yield from add(redis, 'foo', 'bar')

    res = yield from redis.object_encoding('foo')

    if server.version < (3, 0, 0):
        assert res == b'raw'
    else:
        assert res == b'embstr'

    res = yield from redis.incr('key')
    assert res == 1
    res = yield from redis.object_encoding('key')
    assert res == b'int'
    res = yield from redis.object_encoding('non-existent-key')
    assert res is None

    with pytest.raises(TypeError):
        yield from redis.object_encoding(None)


@pytest.mark.run_loop
def test_object_idletime(redis, loop, server):
    yield from add(redis, 'foo', 'bar')

    res = yield from redis.object_idletime('foo')
    assert res == 0

    if server.version < (2, 8, 0):
        # Redis at least 2.6.x requires more time to sleep to incr idletime
        yield from asyncio.sleep(10, loop=loop)
    else:
        yield from asyncio.sleep(1, loop=loop)

    res = yield from redis.object_idletime('foo')
    assert res >= 1

    res = yield from redis.object_idletime('non-existent-key')
    assert res is None

    with pytest.raises(TypeError):
        yield from redis.object_idletime(None)


@pytest.mark.run_loop
def test_persist(redis):
    yield from add(redis, 'my-key', 123)
    res = yield from redis.expire('my-key', 10)
    assert res is True

    res = yield from redis.persist('my-key')
    assert res is True

    res = yield from redis.connection.execute('TTL', 'my-key')
    assert res == -1

    with pytest.raises(TypeError):
        yield from redis.persist(None)


@pytest.mark.run_loop
def test_pexpire(redis, loop):
    yield from add(redis, 'my-key', 123)
    res = yield from redis.pexpire('my-key', 100)
    assert res is True

    res = yield from redis.connection.execute('TTL', 'my-key')
    assert res == 0
    res = yield from redis.connection.execute('PTTL', 'my-key')
    assert res > 0

    yield from add(redis, 'my-key', 123)
    res = yield from redis.pexpire('my-key', 1)
    assert res is True

    yield from asyncio.sleep(.002, loop=loop)

    res = yield from redis.exists('my-key')
    assert not res

    with pytest.raises(TypeError):
        yield from redis.pexpire(None, 0)
    with pytest.raises(TypeError):
        yield from redis.pexpire('my-key', 1.0)


@pytest.mark.run_loop
def test_pexpireat(redis):
    yield from add(redis, 'my-key', 123)
    now = math.ceil((yield from redis.time()) * 1000)
    res = yield from redis.pexpireat('my-key', now + 800)
    assert res is True

    res = yield from redis.ttl('my-key')
    assert res == 1
    res = yield from redis.pttl('my-key')
    pytest.assert_almost_equal(res, 800, -2)

    with pytest.raises(TypeError):
        yield from redis.pexpireat(None, 1234)
    with pytest.raises(TypeError):
        yield from redis.pexpireat('key', 'timestamp')
    with pytest.raises(TypeError):
        yield from redis.pexpireat('key', 1000.0)


@pytest.mark.run_loop
def test_pttl(redis, server):
    yield from add(redis, 'key', 'val')
    res = yield from redis.pttl('key')
    assert res == -1
    res = yield from redis.pttl('non-existent-key')
    if server.version < (2, 8, 0):
        assert res == -1
    else:
        assert res == -2

    yield from redis.pexpire('key', 500)
    res = yield from redis.pttl('key')
    pytest.assert_almost_equal(res, 500, -2)

    with pytest.raises(TypeError):
        yield from redis.pttl(None)


@pytest.mark.run_loop
def test_randomkey(redis):
    yield from add(redis, 'key:1', 123)
    yield from add(redis, 'key:2', 123)
    yield from add(redis, 'key:3', 123)

    res = yield from redis.randomkey()
    assert res in [b'key:1', b'key:2', b'key:3']

    # test with encoding param
    res = yield from redis.randomkey(encoding='utf-8')
    assert res in ['key:1', 'key:2', 'key:3']

    yield from redis.connection.execute('flushdb')
    res = yield from redis.randomkey()
    assert res is None


@pytest.mark.run_loop
def test_rename(redis, server):
    yield from add(redis, 'foo', 'bar')
    yield from redis.delete('bar')

    res = yield from redis.rename('foo', 'bar')
    assert res is True

    with pytest.raises_regex(ReplyError, 'ERR no such key'):
        yield from redis.rename('foo', 'bar')
    with pytest.raises(TypeError):
        yield from redis.rename(None, 'bar')
    with pytest.raises(TypeError):
        yield from redis.rename('foo', None)
    with pytest.raises(ValueError):
        yield from redis.rename('foo', 'foo')

    if server.version < (3, 2):
        with pytest.raises_regex(ReplyError, '.* objects are the same'):
            yield from redis.rename('bar', b'bar')


@pytest.mark.run_loop
def test_renamenx(redis, server):
    yield from redis.delete('foo', 'bar')
    yield from add(redis, 'foo', 123)

    res = yield from redis.renamenx('foo', 'bar')
    assert res is True

    yield from add(redis, 'foo', 123)
    res = yield from redis.renamenx('foo', 'bar')
    assert res is False

    with pytest.raises_regex(ReplyError, 'ERR no such key'):
        yield from redis.renamenx('baz', 'foo')
    with pytest.raises(TypeError):
        yield from redis.renamenx(None, 'foo')
    with pytest.raises(TypeError):
        yield from redis.renamenx('foo', None)
    with pytest.raises(ValueError):
        yield from redis.renamenx('foo', 'foo')

    if server.version < (3, 2):
        with pytest.raises_regex(ReplyError, '.* objects are the same'):
            yield from redis.renamenx('foo', b'foo')


# @pytest.mark.run_loop
@pytest.mark.skip
def test_restore():
    pass


@pytest.redis_version(2, 8, 0, reason='SCAN is available since redis>=2.8.0')
@pytest.mark.run_loop
def test_scan(redis):
    for i in range(1, 11):
        foo_or_bar = 'bar' if i % 3 else 'foo'
        key = 'key:scan:{}:{}'.format(foo_or_bar, i).encode('utf-8')
        yield from add(redis, key, i)

    cursor, values = yield from redis.scan()
    # values should be *>=* just in case some other tests left
    # test keys
    assert len(values) >= 10

    cursor, test_values = b'0', []
    while cursor:
        cursor, values = yield from redis.scan(
            cursor=cursor, match=b'key:scan:foo*')
        test_values.extend(values)
    assert len(test_values) == 3

    cursor, test_values = b'0', []
    while cursor:
        cursor, values = yield from redis.scan(
            cursor=cursor, match=b'key:scan:bar:*')
        test_values.extend(values)
    assert len(test_values) == 7

    # SCAN family functions do not guarantee that the number of
    # elements returned per call are in a given range. So here
    # just dummy test, that *count* argument does not break something
    cursor = b'0'
    test_values = []
    while cursor:
        cursor, values = yield from redis.scan(cursor=cursor,
                                               match=b'key:scan:*',
                                               count=2)

        test_values.extend(values)
    assert len(test_values) == 10


@pytest.mark.run_loop
def test_sort(redis):
    def _make_list(key, items):
        yield from redis.delete(key)
        for i in items:
            yield from redis.rpush(key, i)

    yield from _make_list('a', '4231')
    res = yield from redis.sort('a')
    assert res == [b'1', b'2', b'3', b'4']

    res = yield from redis.sort('a', offset=2, count=2)
    assert res == [b'3', b'4']

    res = yield from redis.sort('a', asc=b'DESC')
    assert res == [b'4', b'3', b'2', b'1']

    yield from _make_list('a', 'dbca')
    res = yield from redis.sort(
        'a', asc=b'DESC', alpha=True, offset=2, count=2
    )
    assert res == [b'b', b'a']

    yield from redis.set('key:1', 10)
    yield from redis.set('key:2', 4)
    yield from redis.set('key:3', 7)
    yield from _make_list('a', '321')

    res = yield from redis.sort('a', by='key:*')
    assert res == [b'2', b'3', b'1']

    res = yield from redis.sort('a', by='nosort')
    assert res == [b'3', b'2', b'1']

    res = yield from redis.sort('a', by='key:*', store='sorted_a')
    assert res == 3
    res = yield from redis.lrange('sorted_a', 0, -1)
    assert res == [b'2', b'3', b'1']

    yield from redis.set('value:1', 20)
    yield from redis.set('value:2', 30)
    yield from redis.set('value:3', 40)
    res = yield from redis.sort('a', 'value:*', by='key:*')
    assert res == [b'30', b'40', b'20']

    yield from redis.hset('data_1', 'weight', 30)
    yield from redis.hset('data_2', 'weight', 20)
    yield from redis.hset('data_3', 'weight', 10)
    yield from redis.hset('hash_1', 'field', 20)
    yield from redis.hset('hash_2', 'field', 30)
    yield from redis.hset('hash_3', 'field', 10)
    res = yield from redis.sort(
        'a', 'hash_*->field', by='data_*->weight'
    )
    assert res == [b'10', b'30', b'20']


@pytest.mark.run_loop
def test_ttl(redis, server):
    yield from add(redis, 'key', 'val')
    res = yield from redis.ttl('key')
    assert res == -1
    res = yield from redis.ttl('non-existent-key')
    if server.version < (2, 8, 0):
        assert res == -1
    else:
        assert res == -2

    yield from redis.expire('key', 10)
    res = yield from redis.ttl('key')
    assert res >= 9

    with pytest.raises(TypeError):
        yield from redis.ttl(None)


@pytest.mark.run_loop
def test_type(redis):
    yield from add(redis, 'key', 'val')
    res = yield from redis.type('key')
    assert res == b'string'

    yield from redis.delete('key')
    yield from redis.incr('key')
    res = yield from redis.type('key')
    assert res == b'string'

    yield from redis.delete('key')
    yield from redis.sadd('key', 'val')
    res = yield from redis.type('key')
    assert res == b'set'

    res = yield from redis.type('non-existent-key')
    assert res == b'none'

    with pytest.raises(TypeError):
        yield from redis.type(None)
