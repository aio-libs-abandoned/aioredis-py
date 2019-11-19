import asyncio
import time
import math
import pytest
import sys

from unittest import mock

from aioredis import ReplyError
from _testutils import redis_version


async def add(redis, key, value):
    ok = await redis.connection.execute('set', key, value)
    assert ok == b'OK'


async def test_delete(redis):
    await add(redis, 'my-key', 123)
    await add(redis, 'other-key', 123)

    res = await redis.delete('my-key', 'non-existent-key')
    assert res == 1

    res = await redis.delete('other-key', 'other-key')
    assert res == 1

    with pytest.raises(TypeError):
        await redis.delete(None)

    with pytest.raises(TypeError):
        await redis.delete('my-key', 'my-key', None)


async def test_dump(redis):
    await add(redis, 'my-key', 123)

    data = await redis.dump('my-key')
    assert data == mock.ANY
    assert isinstance(data, (bytes, bytearray))
    assert len(data) > 0

    data = await redis.dump('non-existent-key')
    assert data is None

    with pytest.raises(TypeError):
        await redis.dump(None)


async def test_exists(redis, server):
    await add(redis, 'my-key', 123)

    res = await redis.exists('my-key')
    assert isinstance(res, int)
    assert res == 1

    res = await redis.exists('non-existent-key')
    assert isinstance(res, int)
    assert res == 0

    with pytest.raises(TypeError):
        await redis.exists(None)
    if server.version < (3, 0, 3):
        with pytest.raises(ReplyError):
            await redis.exists('key-1', 'key-2')


@redis_version(
    3, 0, 3, reason='Multi-key EXISTS available since redis>=2.8.0')
async def test_exists_multiple(redis):
    await add(redis, 'my-key', 123)

    res = await redis.exists('my-key', 'other-key')
    assert isinstance(res, int)
    assert res == 1

    res = await redis.exists('my-key', 'my-key')
    assert isinstance(res, int)
    assert res == 2

    res = await redis.exists('foo', 'bar')
    assert isinstance(res, int)
    assert res == 0


async def test_expire(redis):
    await add(redis, 'my-key', 132)

    res = await redis.expire('my-key', 10)
    assert res is True

    res = await redis.connection.execute('TTL', 'my-key')
    assert res >= 10

    await redis.expire('my-key', -1)
    res = await redis.exists('my-key')
    assert not res

    res = await redis.expire('other-key', 1000)
    assert res is False

    await add(redis, 'my-key', 1)
    res = await redis.expire('my-key', 10.0)
    assert res is True
    res = await redis.connection.execute('TTL', 'my-key')
    assert res >= 10

    with pytest.raises(TypeError):
        await redis.expire(None, 123)
    with pytest.raises(TypeError):
        await redis.expire('my-key', 'timeout')


async def test_expireat(redis):
    await add(redis, 'my-key', 123)
    now = math.ceil(time.time())

    fut1 = redis.expireat('my-key', now + 10)
    fut2 = redis.connection.execute('TTL', 'my-key')
    assert (await fut1) is True
    assert (await fut2) >= 10

    now = time.time()
    fut1 = redis.expireat('my-key', now + 10)
    fut2 = redis.connection.execute('TTL', 'my-key')
    assert (await fut1) is True
    assert (await fut2) >= 10

    res = await redis.expireat('my-key', -1)
    assert res is True

    res = await redis.exists('my-key')
    assert not res

    await add(redis, 'my-key', 123)

    res = await redis.expireat('my-key', 0)
    assert res is True

    res = await redis.exists('my-key')
    assert not res

    await add(redis, 'my-key', 123)
    with pytest.raises(TypeError):
        await redis.expireat(None, 123)
    with pytest.raises(TypeError):
        await redis.expireat('my-key', 'timestamp')


async def test_keys(redis):
    res = await redis.keys('*pattern*')
    assert res == []

    await redis.connection.execute('FLUSHDB')
    res = await redis.keys('*')
    assert res == []

    await add(redis, 'my-key-1', 1)
    await add(redis, 'my-key-ab', 1)

    res = await redis.keys('my-key-?')
    assert res == [b'my-key-1']
    res = await redis.keys('my-key-*')
    assert sorted(res) == [b'my-key-1', b'my-key-ab']

    # test with encoding param
    res = await redis.keys('my-key-*', encoding='utf-8')
    assert sorted(res) == ['my-key-1', 'my-key-ab']

    with pytest.raises(TypeError):
        await redis.keys(None)


async def test_migrate(create_redis, server, serverB):
    redisA = await create_redis(server.tcp_address)
    redisB = await create_redis(serverB.tcp_address, db=2)

    await add(redisA, 'my-key', 123)

    await redisB.delete('my-key')
    assert (await redisA.exists('my-key'))
    assert not (await redisB.exists('my-key'))

    ok = await redisA.migrate(
        'localhost', serverB.tcp_address.port, 'my-key', 2, 1000)
    assert ok is True
    assert not (await redisA.exists('my-key'))
    assert (await redisB.exists('my-key'))

    with pytest.raises(TypeError, match="host .* str"):
        await redisA.migrate(None, 1234, 'key', 1, 23)
    with pytest.raises(TypeError, match="args .* None"):
        await redisA.migrate('host', '1234',  None, 1, 123)
    with pytest.raises(TypeError, match="dest_db .* int"):
        await redisA.migrate('host', 123, 'key', 1.0, 123)
    with pytest.raises(TypeError, match="timeout .* int"):
        await redisA.migrate('host', '1234', 'key', 2, None)
    with pytest.raises(ValueError, match="Got empty host"):
        await redisA.migrate('', '123', 'key', 1, 123)
    with pytest.raises(ValueError, match="dest_db .* greater equal 0"):
        await redisA.migrate('host', 6379, 'key', -1, 1000)
    with pytest.raises(ValueError, match="timeout .* greater equal 0"):
        await redisA.migrate('host', 6379, 'key', 1, -1000)


@redis_version(
    3, 0, 0, reason="Copy/Replace flags available since Redis 3.0")
async def test_migrate_copy_replace(create_redis, server, serverB):
    redisA = await create_redis(server.tcp_address)
    redisB = await create_redis(serverB.tcp_address, db=0)

    await add(redisA, 'my-key', 123)
    await redisB.delete('my-key')

    ok = await redisA.migrate(
        'localhost', serverB.tcp_address.port, 'my-key', 0, 1000, copy=True)
    assert ok is True
    assert (await redisA.get('my-key')) == b'123'
    assert (await redisB.get('my-key')) == b'123'

    assert (await redisA.set('my-key', 'val'))
    ok = await redisA.migrate(
        'localhost', serverB.tcp_address.port, 'my-key', 2, 1000, replace=True)
    assert (await redisA.get('my-key')) is None
    assert (await redisB.get('my-key'))


@redis_version(
    3, 0, 6, reason="MIGRATE…KEYS available since Redis 3.0.6")
@pytest.mark.skipif(
    sys.platform == 'win32', reason="Seems to be unavailable in win32 build")
async def test_migrate_keys(create_redis, server, serverB):
    redisA = await create_redis(server.tcp_address)
    redisB = await create_redis(serverB.tcp_address, db=0)

    await add(redisA, 'key1', 123)
    await add(redisA, 'key2', 123)
    await add(redisA, 'key3', 123)
    await redisB.delete('key1', 'key2', 'key3')

    ok = await redisA.migrate_keys(
        'localhost', serverB.tcp_address.port,
        ('key1', 'key2', 'key3', 'non-existing-key'),
        dest_db=0, timeout=1000)
    assert ok is True

    assert (await redisB.get('key1')) == b'123'
    assert (await redisB.get('key2')) == b'123'
    assert (await redisB.get('key3')) == b'123'
    assert (await redisA.get('key1')) is None
    assert (await redisA.get('key2')) is None
    assert (await redisA.get('key3')) is None

    ok = await redisA.migrate_keys(
        'localhost', serverB.tcp_address.port, ('key1', 'key2', 'key3'),
        dest_db=0, timeout=1000)
    assert not ok
    ok = await redisB.migrate_keys(
        'localhost', server.tcp_address.port, ('key1', 'key2', 'key3'),
        dest_db=0, timeout=1000,
        copy=True)
    assert ok
    assert (await redisB.get('key1')) == b'123'
    assert (await redisB.get('key2')) == b'123'
    assert (await redisB.get('key3')) == b'123'
    assert (await redisA.get('key1')) == b'123'
    assert (await redisA.get('key2')) == b'123'
    assert (await redisA.get('key3')) == b'123'

    assert (await redisA.set('key1', 'val'))
    assert (await redisA.set('key2', 'val'))
    assert (await redisA.set('key3', 'val'))
    ok = await redisA.migrate_keys(
        'localhost', serverB.tcp_address.port,
        ('key1', 'key2', 'key3', 'non-existing-key'),
        dest_db=0, timeout=1000, replace=True)
    assert ok is True

    assert (await redisB.get('key1')) == b'val'
    assert (await redisB.get('key2')) == b'val'
    assert (await redisB.get('key3')) == b'val'
    assert (await redisA.get('key1')) is None
    assert (await redisA.get('key2')) is None
    assert (await redisA.get('key3')) is None


async def test_migrate__exceptions(redis, server, unused_port):
    await add(redis, 'my-key', 123)

    assert (await redis.exists('my-key'))

    with pytest.raises(ReplyError, match="IOERR .* timeout .*"):
        assert not (await redis.migrate(
            'localhost', unused_port(),
            'my-key', dest_db=30, timeout=10))


@redis_version(
    3, 0, 6, reason="MIGRATE…KEYS available since Redis 3.0.6")
@pytest.mark.skipif(
    sys.platform == 'win32', reason="Seems to be unavailable in win32 build")
async def test_migrate_keys__errors(redis):
    with pytest.raises(TypeError, match="host .* str"):
        await redis.migrate_keys(None, 1234, 'key', 1, 23)
    with pytest.raises(TypeError, match="keys .* list or tuple"):
        await redis.migrate_keys('host', '1234',  None, 1, 123)
    with pytest.raises(TypeError, match="dest_db .* int"):
        await redis.migrate_keys('host', 123, ('key',), 1.0, 123)
    with pytest.raises(TypeError, match="timeout .* int"):
        await redis.migrate_keys('host', '1234', ('key',), 2, None)
    with pytest.raises(ValueError, match="Got empty host"):
        await redis.migrate_keys('', '123', ('key',), 1, 123)
    with pytest.raises(ValueError, match="dest_db .* greater equal 0"):
        await redis.migrate_keys('host', 6379, ('key',), -1, 1000)
    with pytest.raises(ValueError, match="timeout .* greater equal 0"):
        await redis.migrate_keys('host', 6379, ('key',), 1, -1000)
    with pytest.raises(ValueError, match="keys .* empty"):
        await redis.migrate_keys('host', '1234', (), 2, 123)


async def test_move(redis):
    await add(redis, 'my-key', 123)

    assert redis.db == 0
    res = await redis.move('my-key', 1)
    assert res is True

    with pytest.raises(TypeError):
        await redis.move(None, 1)
    with pytest.raises(TypeError):
        await redis.move('my-key', None)
    with pytest.raises(ValueError):
        await redis.move('my-key', -1)
    with pytest.raises(TypeError):
        await redis.move('my-key', 'not db')


async def test_object_refcount(redis):
    await add(redis, 'foo', 'bar')

    res = await redis.object_refcount('foo')
    assert res == 1
    res = await redis.object_refcount('non-existent-key')
    assert res is None

    with pytest.raises(TypeError):
        await redis.object_refcount(None)


async def test_object_encoding(redis, server):
    await add(redis, 'foo', 'bar')

    res = await redis.object_encoding('foo')

    if server.version < (3, 0, 0):
        assert res == 'raw'
    else:
        assert res == 'embstr'

    res = await redis.incr('key')
    assert res == 1
    res = await redis.object_encoding('key')
    assert res == 'int'
    res = await redis.object_encoding('non-existent-key')
    assert res is None

    with pytest.raises(TypeError):
        await redis.object_encoding(None)


@redis_version(
    3, 0, 0, reason="Older Redis version has lower idle time resolution")
@pytest.mark.timeout(20)
async def test_object_idletime(redis, server):
    await add(redis, 'foo', 'bar')

    res = await redis.object_idletime('foo')
    # NOTE: sometimes travis-ci is too slow
    assert res >= 0

    res = 0
    while not res:
        res = await redis.object_idletime('foo')
        await asyncio.sleep(.5)
    assert res >= 1

    res = await redis.object_idletime('non-existent-key')
    assert res is None

    with pytest.raises(TypeError):
        await redis.object_idletime(None)


async def test_persist(redis):
    await add(redis, 'my-key', 123)
    res = await redis.expire('my-key', 10)
    assert res is True

    res = await redis.persist('my-key')
    assert res is True

    res = await redis.connection.execute('TTL', 'my-key')
    assert res == -1

    with pytest.raises(TypeError):
        await redis.persist(None)


async def test_pexpire(redis):
    await add(redis, 'my-key', 123)
    res = await redis.pexpire('my-key', 100)
    assert res is True

    res = await redis.connection.execute('TTL', 'my-key')
    assert res == 0
    res = await redis.connection.execute('PTTL', 'my-key')
    assert res > 0

    await add(redis, 'my-key', 123)
    res = await redis.pexpire('my-key', 1)
    assert res is True

    # XXX: tests now looks strange to me.
    await asyncio.sleep(.2)

    res = await redis.exists('my-key')
    assert not res

    with pytest.raises(TypeError):
        await redis.pexpire(None, 0)
    with pytest.raises(TypeError):
        await redis.pexpire('my-key', 1.0)


async def test_pexpireat(redis):
    await add(redis, 'my-key', 123)
    now = int((await redis.time()) * 1000)
    fut1 = redis.pexpireat('my-key', now + 2000)
    fut2 = redis.ttl('my-key')
    fut3 = redis.pttl('my-key')
    assert await fut1 is True
    assert await fut2 == 2
    assert 1000 < await fut3 <= 2000

    with pytest.raises(TypeError):
        await redis.pexpireat(None, 1234)
    with pytest.raises(TypeError):
        await redis.pexpireat('key', 'timestamp')
    with pytest.raises(TypeError):
        await redis.pexpireat('key', 1000.0)


async def test_pttl(redis, server):
    await add(redis, 'key', 'val')
    res = await redis.pttl('key')
    assert res == -1
    res = await redis.pttl('non-existent-key')
    if server.version < (2, 8, 0):
        assert res == -1
    else:
        assert res == -2

    await redis.pexpire('key', 500)
    res = await redis.pttl('key')
    assert 400 < res <= 500

    with pytest.raises(TypeError):
        await redis.pttl(None)


async def test_randomkey(redis):
    await add(redis, 'key:1', 123)
    await add(redis, 'key:2', 123)
    await add(redis, 'key:3', 123)

    res = await redis.randomkey()
    assert res in [b'key:1', b'key:2', b'key:3']

    # test with encoding param
    res = await redis.randomkey(encoding='utf-8')
    assert res in ['key:1', 'key:2', 'key:3']

    await redis.connection.execute('flushdb')
    res = await redis.randomkey()
    assert res is None


async def test_rename(redis, server):
    await add(redis, 'foo', 'bar')
    await redis.delete('bar')

    res = await redis.rename('foo', 'bar')
    assert res is True

    with pytest.raises(ReplyError, match='ERR no such key'):
        await redis.rename('foo', 'bar')
    with pytest.raises(TypeError):
        await redis.rename(None, 'bar')
    with pytest.raises(TypeError):
        await redis.rename('foo', None)
    with pytest.raises(ValueError):
        await redis.rename('foo', 'foo')

    if server.version < (3, 2):
        with pytest.raises(ReplyError, match='.* objects are the same'):
            await redis.rename('bar', b'bar')


async def test_renamenx(redis, server):
    await redis.delete('foo', 'bar')
    await add(redis, 'foo', 123)

    res = await redis.renamenx('foo', 'bar')
    assert res is True

    await add(redis, 'foo', 123)
    res = await redis.renamenx('foo', 'bar')
    assert res is False

    with pytest.raises(ReplyError, match='ERR no such key'):
        await redis.renamenx('baz', 'foo')
    with pytest.raises(TypeError):
        await redis.renamenx(None, 'foo')
    with pytest.raises(TypeError):
        await redis.renamenx('foo', None)
    with pytest.raises(ValueError):
        await redis.renamenx('foo', 'foo')

    if server.version < (3, 2):
        with pytest.raises(ReplyError, match='.* objects are the same'):
            await redis.renamenx('foo', b'foo')


async def test_restore(redis):
    ok = await redis.set('key', 'value')
    assert ok
    dump = await redis.dump('key')
    assert dump is not None
    ok = await redis.delete('key')
    assert ok
    assert b'OK' == (await redis.restore('key', 0, dump))
    assert (await redis.get('key')) == b'value'


@redis_version(2, 8, 0, reason='SCAN is available since redis>=2.8.0')
async def test_scan(redis):
    for i in range(1, 11):
        foo_or_bar = 'bar' if i % 3 else 'foo'
        key = 'key:scan:{}:{}'.format(foo_or_bar, i).encode('utf-8')
        await add(redis, key, i)

    cursor, values = await redis.scan()
    # values should be *>=* just in case some other tests left
    # test keys
    assert len(values) >= 10

    cursor, test_values = b'0', []
    while cursor:
        cursor, values = await redis.scan(
            cursor=cursor, match=b'key:scan:foo*')
        test_values.extend(values)
    assert len(test_values) == 3

    cursor, test_values = b'0', []
    while cursor:
        cursor, values = await redis.scan(
            cursor=cursor, match=b'key:scan:bar:*')
        test_values.extend(values)
    assert len(test_values) == 7

    # SCAN family functions do not guarantee that the number of
    # elements returned per call are in a given range. So here
    # just dummy test, that *count* argument does not break something
    cursor = b'0'
    test_values = []
    while cursor:
        cursor, values = await redis.scan(cursor=cursor,
                                          match=b'key:scan:*',
                                          count=2)

        test_values.extend(values)
    assert len(test_values) == 10


async def test_sort(redis):
    async def _make_list(key, items):
        await redis.delete(key)
        for i in items:
            await redis.rpush(key, i)

    await _make_list('a', '4231')
    res = await redis.sort('a')
    assert res == [b'1', b'2', b'3', b'4']

    res = await redis.sort('a', offset=2, count=2)
    assert res == [b'3', b'4']

    res = await redis.sort('a', asc=b'DESC')
    assert res == [b'4', b'3', b'2', b'1']

    await _make_list('a', 'dbca')
    res = await redis.sort(
        'a', asc=b'DESC', alpha=True, offset=2, count=2
    )
    assert res == [b'b', b'a']

    await redis.set('key:1', 10)
    await redis.set('key:2', 4)
    await redis.set('key:3', 7)
    await _make_list('a', '321')

    res = await redis.sort('a', by='key:*')
    assert res == [b'2', b'3', b'1']

    res = await redis.sort('a', by='nosort')
    assert res == [b'3', b'2', b'1']

    res = await redis.sort('a', by='key:*', store='sorted_a')
    assert res == 3
    res = await redis.lrange('sorted_a', 0, -1)
    assert res == [b'2', b'3', b'1']

    await redis.set('value:1', 20)
    await redis.set('value:2', 30)
    await redis.set('value:3', 40)
    res = await redis.sort('a', 'value:*', by='key:*')
    assert res == [b'30', b'40', b'20']

    await redis.hset('data_1', 'weight', 30)
    await redis.hset('data_2', 'weight', 20)
    await redis.hset('data_3', 'weight', 10)
    await redis.hset('hash_1', 'field', 20)
    await redis.hset('hash_2', 'field', 30)
    await redis.hset('hash_3', 'field', 10)
    res = await redis.sort(
        'a', 'hash_*->field', by='data_*->weight'
    )
    assert res == [b'10', b'30', b'20']


@redis_version(3, 2, 1, reason="TOUCH is available since redis>=3.2.1")
@pytest.mark.timeout(20)
async def test_touch(redis):
    await add(redis, 'key', 'val')
    res = 0
    while not res:
        res = await redis.object_idletime('key')
        await asyncio.sleep(.5)
    assert res > 0
    assert await redis.touch('key', 'key', 'key') == 3
    res2 = await redis.object_idletime('key')
    assert 0 <= res2 < res


async def test_ttl(redis, server):
    await add(redis, 'key', 'val')
    res = await redis.ttl('key')
    assert res == -1
    res = await redis.ttl('non-existent-key')
    if server.version < (2, 8, 0):
        assert res == -1
    else:
        assert res == -2

    await redis.expire('key', 10)
    res = await redis.ttl('key')
    assert res >= 9

    with pytest.raises(TypeError):
        await redis.ttl(None)


async def test_type(redis):
    await add(redis, 'key', 'val')
    res = await redis.type('key')
    assert res == b'string'

    await redis.delete('key')
    await redis.incr('key')
    res = await redis.type('key')
    assert res == b'string'

    await redis.delete('key')
    await redis.sadd('key', 'val')
    res = await redis.type('key')
    assert res == b'set'

    res = await redis.type('non-existent-key')
    assert res == b'none'

    with pytest.raises(TypeError):
        await redis.type(None)


@redis_version(2, 8, 0, reason='SCAN is available since redis>=2.8.0')
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


@redis_version(4, 0, 0, reason="UNLINK is available since redis>=4.0.0")
async def test_unlink(redis):
    await add(redis, 'my-key', 123)
    await add(redis, 'other-key', 123)

    res = await redis.unlink('my-key', 'non-existent-key')
    assert res == 1

    res = await redis.unlink('other-key', 'other-key')
    assert res == 1

    with pytest.raises(TypeError):
        await redis.unlink(None)

    with pytest.raises(TypeError):
        await redis.unlink('my-key', 'my-key', None)


@redis_version(3, 0, 0, reason="WAIT is available since redis>=3.0.0")
async def test_wait(redis):
    await add(redis, 'key', 'val1')
    start = await redis.time()
    res = await redis.wait(1, 400)
    end = await redis.time()
    assert res == 0
    assert end - start >= .4

    await add(redis, 'key', 'val2')
    start = await redis.time()
    res = await redis.wait(0, 400)
    end = await redis.time()
    assert res == 0
    assert end - start < .4
