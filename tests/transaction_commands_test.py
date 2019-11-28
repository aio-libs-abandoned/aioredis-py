import asyncio
import pytest

from aioredis import ReplyError, MultiExecError, WatchVariableError
from aioredis import ConnectionClosedError


async def test_multi_exec(redis):
    await redis.delete('foo', 'bar')

    tr = redis.multi_exec()
    f1 = tr.incr('foo')
    f2 = tr.incr('bar')
    res = await tr.execute()
    assert res == [1, 1]
    res2 = await asyncio.gather(f1, f2)
    assert res == res2

    tr = redis.multi_exec()
    f1 = tr.incr('foo')
    f2 = tr.incr('bar')
    await tr.execute()
    assert (await f1) == 2
    assert (await f2) == 2

    tr = redis.multi_exec()
    f1 = tr.set('foo', 1.0)
    f2 = tr.incrbyfloat('foo', 1.2)
    res = await tr.execute()
    assert res == [True, 2.2]
    res2 = await asyncio.gather(f1, f2)
    assert res == res2

    tr = redis.multi_exec()
    f1 = tr.incrby('foo', 1.0)
    with pytest.raises(MultiExecError, match="increment must be .* int"):
        await tr.execute()
    with pytest.raises(TypeError):
        await f1


async def test_empty(redis):
    tr = redis.multi_exec()
    res = await tr.execute()
    assert res == []


async def test_double_execute(redis):
    tr = redis.multi_exec()
    await tr.execute()
    with pytest.raises(AssertionError):
        await tr.execute()
    with pytest.raises(AssertionError):
        await tr.incr('foo')


async def test_connection_closed(redis):
    tr = redis.multi_exec()
    fut1 = tr.quit()
    fut2 = tr.incrby('foo', 1.0)
    fut3 = tr.incrby('foo', 1)
    with pytest.raises(MultiExecError):
        await tr.execute()

    assert fut1.done() is True
    assert fut2.done() is True
    assert fut3.done() is True
    assert fut1.exception() is not None
    assert fut2.exception() is not None
    assert fut3.exception() is not None
    assert not fut1.cancelled()
    assert not fut2.cancelled()
    assert not fut3.cancelled()

    try:
        assert (await fut1) == b'OK'
    except Exception as err:
        assert isinstance(err, (ConnectionClosedError, ConnectionError))
    assert fut2.cancelled() is False
    assert isinstance(fut2.exception(), TypeError)

    # assert fut3.cancelled() is True
    assert fut3.done() and not fut3.cancelled()
    assert isinstance(fut3.exception(),
                      (ConnectionClosedError, ConnectionError))


async def test_discard(redis):
    await redis.delete('foo')
    tr = redis.multi_exec()
    fut1 = tr.incrby('foo', 1.0)
    fut2 = tr.connection.execute('MULTI')
    fut3 = tr.connection.execute('incr', 'foo')

    with pytest.raises(MultiExecError):
        await tr.execute()
    with pytest.raises(TypeError):
        await fut1
    with pytest.raises(ReplyError):
        await fut2
    # with pytest.raises(ReplyError):
    res = await fut3
    assert res == 1


async def test_exec_error(redis):
    tr = redis.multi_exec()
    fut = tr.connection.execute('INCRBY', 'key', '1.0')
    with pytest.raises(MultiExecError):
        await tr.execute()
    with pytest.raises(ReplyError):
        await fut

    await redis.set('foo', 'bar')
    tr = redis.multi_exec()
    fut = tr.incrbyfloat('foo', 1.1)
    res = await tr.execute(return_exceptions=True)
    assert isinstance(res[0], ReplyError)
    with pytest.raises(ReplyError):
        await fut


async def test_command_errors(redis):
    tr = redis.multi_exec()
    fut = tr.incrby('key', 1.0)
    with pytest.raises(MultiExecError):
        await tr.execute()
    with pytest.raises(TypeError):
        await fut


async def test_several_command_errors(redis):
    tr = redis.multi_exec()
    fut1 = tr.incrby('key', 1.0)
    fut2 = tr.rename('bar', 'bar')
    with pytest.raises(MultiExecError):
        await tr.execute()
    with pytest.raises(TypeError):
        await fut1
    with pytest.raises(ValueError):
        await fut2


async def test_error_in_connection(redis):
    await redis.set('foo', 1)
    tr = redis.multi_exec()
    fut1 = tr.mget('foo', None)
    fut2 = tr.incr('foo')
    with pytest.raises(MultiExecError):
        await tr.execute()
    with pytest.raises(TypeError):
        await fut1
    await fut2


async def test_watch_unwatch(redis):
    res = await redis.watch('key')
    assert res is True
    res = await redis.watch('key', 'key')
    assert res is True

    with pytest.raises(TypeError):
        await redis.watch(None)
    with pytest.raises(TypeError):
        await redis.watch('key', None)
    with pytest.raises(TypeError):
        await redis.watch('key', 'key', None)

    res = await redis.unwatch()
    assert res is True


async def test_encoding(redis):
    res = await redis.set('key', 'value')
    assert res is True
    res = await redis.hmset(
        'hash-key', 'foo', 'val1', 'bar', 'val2')
    assert res is True

    tr = redis.multi_exec()
    fut1 = tr.get('key')
    fut2 = tr.get('key', encoding='utf-8')
    fut3 = tr.hgetall('hash-key', encoding='utf-8')
    await tr.execute()
    res = await fut1
    assert res == b'value'
    res = await fut2
    assert res == 'value'
    res = await fut3
    assert res == {'foo': 'val1', 'bar': 'val2'}


async def test_global_encoding(redis, create_redis, server):
    redis = await create_redis(server.tcp_address, encoding='utf-8')
    res = await redis.set('key', 'value')
    assert res is True
    res = await redis.hmset(
        'hash-key', 'foo', 'val1', 'bar', 'val2')
    assert res is True

    tr = redis.multi_exec()
    fut1 = tr.get('key')
    fut2 = tr.get('key', encoding='utf-8')
    fut3 = tr.get('key', encoding=None)
    fut4 = tr.hgetall('hash-key', encoding='utf-8')
    await tr.execute()
    res = await fut1
    assert res == 'value'
    res = await fut2
    assert res == 'value'
    res = await fut3
    assert res == b'value'
    res = await fut4
    assert res == {'foo': 'val1', 'bar': 'val2'}


async def test_transaction__watch_error(redis, create_redis, server):
    other = await create_redis(server.tcp_address)

    ok = await redis.set('foo', 'bar')
    assert ok is True

    ok = await redis.watch('foo')
    assert ok is True

    ok = await other.set('foo', 'baz')
    assert ok is True

    tr = redis.multi_exec()
    fut1 = tr.set('foo', 'foo')
    fut2 = tr.get('bar')
    with pytest.raises(MultiExecError):
        await tr.execute()
    with pytest.raises(WatchVariableError):
        await fut1
    with pytest.raises(WatchVariableError):
        await fut2


async def test_multi_exec_and_pool_release(redis):
    # Test the case when pool connection is released before
    # `exec` result is received.

    slow_script = """
    local a = tonumber(redis.call('time')[1])
    local b = a + 1
    while (a < b)
    do
        a = tonumber(redis.call('time')[1])
    end
    """

    tr = redis.multi_exec()
    fut1 = tr.eval(slow_script)
    ret, = await tr.execute()
    assert ret is None
    assert (await fut1) is None


async def test_multi_exec_db_select(redis):
    await redis.set('foo', 'bar')

    tr = redis.multi_exec()
    f1 = tr.get('foo', encoding='utf-8')
    f2 = tr.get('foo')
    await tr.execute()
    assert await f1 == 'bar'
    assert await f2 == b'bar'
