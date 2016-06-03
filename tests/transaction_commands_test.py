import asyncio
import pytest

from aioredis import ReplyError, MultiExecError, WatchVariableError


@pytest.mark.run_loop
def test_multi_exec(redis, loop):
    yield from redis.delete('foo', 'bar')

    tr = redis.multi_exec()
    f1 = tr.incr('foo')
    f2 = tr.incr('bar')
    res = yield from tr.execute()
    assert res == [1, 1]
    res2 = yield from asyncio.gather(f1, f2, loop=loop)
    assert res == res2

    tr = redis.multi_exec()
    f1 = tr.incr('foo')
    f2 = tr.incr('bar')
    yield from tr.execute()
    assert (yield from f1) == 2
    assert (yield from f2) == 2

    tr = redis.multi_exec()
    f1 = tr.set('foo', 1.0)
    f2 = tr.incrbyfloat('foo', 1.2)
    res = yield from tr.execute()
    assert res == [True, 2.2]
    res2 = yield from asyncio.gather(f1, f2, loop=loop)
    assert res == res2

    tr = redis.multi_exec()
    f1 = tr.incrby('foo', 1.0)
    with pytest.raises_regex(MultiExecError, "increment must be .* int"):
        yield from tr.execute()
    with pytest.raises(TypeError):
        yield from f1


@pytest.mark.run_loop
def test_empty(redis):
    tr = redis.multi_exec()
    res = yield from tr.execute()
    assert res == []


@pytest.mark.run_loop
def test_double_execute(redis):
    tr = redis.multi_exec()
    yield from tr.execute()
    with pytest.raises(AssertionError):
        yield from tr.execute()
    with pytest.raises(AssertionError):
        yield from tr.incr('foo')


@pytest.mark.run_loop
def test_connection_closed(redis):
    tr = redis.multi_exec()
    fut1 = tr.quit()
    fut2 = tr.incrby('foo', 1.0)
    fut3 = tr.connection.execute('INCRBY', 'foo', '1.0')
    with pytest.raises(MultiExecError):
        yield from tr.execute()

    assert fut1.done() is True
    assert fut2.done() is True
    assert fut3.done() is True

    try:
        res = yield from fut1
        assert res == b'OK'
    except asyncio.CancelledError:
        pass
    assert fut2.cancelled() is False
    assert isinstance(fut2.exception(), TypeError)

    assert fut3.cancelled() is True


@pytest.mark.run_loop
def test_discard(redis):
    yield from redis.delete('foo')
    tr = redis.multi_exec()
    fut1 = tr.incrby('foo', 1.0)
    fut2 = tr.connection.execute('MULTI')
    fut3 = tr.connection.execute('incr', 'foo')

    with pytest.raises(ReplyError):
        yield from tr.execute()
    with pytest.raises(TypeError):
        yield from fut1
    with pytest.raises(ReplyError):
        yield from fut2
    # with pytest.raises(ReplyError):
    res = yield from fut3
    assert res == 1


@pytest.mark.run_loop
def test_exec_error(redis):
    tr = redis.multi_exec()
    fut = tr.connection.execute('INCRBY', 'key', '1.0')
    with pytest.raises(MultiExecError):
        yield from tr.execute()
    with pytest.raises(ReplyError):
        yield from fut

    yield from redis.set('foo', 'bar')
    tr = redis.multi_exec()
    fut = tr.incrbyfloat('foo', 1.1)
    res = yield from tr.execute(return_exceptions=True)
    assert isinstance(res[0], ReplyError)
    with pytest.raises(ReplyError):
        yield from fut


@pytest.mark.run_loop
def test_command_errors(redis):
    tr = redis.multi_exec()
    fut = tr.incrby('key', 1.0)
    with pytest.raises(MultiExecError):
        yield from tr.execute()
    with pytest.raises(TypeError):
        yield from fut


@pytest.mark.run_loop
def test_several_command_errors(redis):
    tr = redis.multi_exec()
    fut1 = tr.incrby('key', 1.0)
    fut2 = tr.rename('bar', 'bar')
    with pytest.raises(MultiExecError):
        yield from tr.execute()
    with pytest.raises(TypeError):
        yield from fut1
    with pytest.raises(ValueError):
        yield from fut2


@pytest.mark.run_loop
def test_error_in_connection(redis):
    yield from redis.set('foo', 1)
    tr = redis.multi_exec()
    fut1 = tr.mget('foo', None)
    fut2 = tr.incr('foo')
    with pytest.raises(MultiExecError):
        yield from tr.execute()
    with pytest.raises(TypeError):
        yield from fut1
    yield from fut2


@pytest.mark.run_loop
def test_watch_unwatch(redis):
    res = yield from redis.watch('key')
    assert res is True
    res = yield from redis.watch('key', 'key')
    assert res is True

    with pytest.raises(TypeError):
        yield from redis.watch(None)
    with pytest.raises(TypeError):
        yield from redis.watch('key', None)
    with pytest.raises(TypeError):
        yield from redis.watch('key', 'key', None)

    res = yield from redis.unwatch()
    assert res is True


@pytest.mark.run_loop
def test_encoding(redis):
    res = yield from redis.set('key', 'value')
    assert res is True
    res = yield from redis.hmset(
        'hash-key', 'foo', 'val1', 'bar', 'val2')
    assert res is True

    tr = redis.multi_exec()
    fut1 = tr.get('key')
    fut2 = tr.get('key', encoding='utf-8')
    fut3 = tr.hgetall('hash-key', encoding='utf-8')
    yield from tr.execute()
    res = yield from fut1
    assert res == b'value'
    res = yield from fut2
    assert res == 'value'
    res = yield from fut3
    assert res == {'foo': 'val1', 'bar': 'val2'}


@pytest.mark.run_loop
def test_global_encoding(redis, create_redis, server, loop):
    redis = yield from create_redis(
        server.tcp_address,
        loop=loop, encoding='utf-8')
    res = yield from redis.set('key', 'value')
    assert res is True
    res = yield from redis.hmset(
        'hash-key', 'foo', 'val1', 'bar', 'val2')
    assert res is True

    tr = redis.multi_exec()
    fut1 = tr.get('key')
    fut2 = tr.get('key', encoding='utf-8')
    fut3 = tr.hgetall('hash-key', encoding='utf-8')
    yield from tr.execute()
    res = yield from fut1
    assert res == 'value'
    res = yield from fut2
    assert res == 'value'
    res = yield from fut3
    assert res == {'foo': 'val1', 'bar': 'val2'}


@pytest.mark.run_loop
def test_transaction__watch_error(redis, create_redis, server, loop):
    other = yield from create_redis(
        server.tcp_address, loop=loop)

    ok = yield from redis.set('foo', 'bar')
    assert ok is True

    ok = yield from redis.watch('foo')
    assert ok is True

    ok = yield from other.set('foo', 'baz')
    assert ok is True

    tr = redis.multi_exec()
    fut1 = tr.set('foo', 'foo')
    fut2 = tr.get('bar')
    with pytest.raises(MultiExecError):
        yield from tr.execute()
    with pytest.raises(WatchVariableError):
        yield from fut1
    with pytest.raises(WatchVariableError):
        yield from fut2
