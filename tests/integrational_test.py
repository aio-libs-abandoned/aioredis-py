import asyncio
import pytest

import aioredis
from aioredis.util import async_task


@pytest.fixture
def pool_or_redis(_closable, server, loop):
    version = tuple(map(int, aioredis.__version__.split('.')[:2]))
    if version >= (1, 0):
        factory = aioredis.create_redis_pool
    else:
        factory = aioredis.create_pool

    @asyncio.coroutine
    def redis_factory(maxsize):
        redis = yield from factory(server.tcp_address, loop=loop,
                                   minsize=1, maxsize=maxsize)
        _closable(redis)
        return redis
    return redis_factory


@asyncio.coroutine
def simple_get_set(pool, idx, loop):
    """A simple test to make sure Redis(pool) can be used as old Pool(Redis).
    """
    val = 'val:{}'.format(idx)
    with (yield from pool) as redis:
        assert (yield from redis.set('key', val))
        yield from redis.get('key', encoding='utf-8')


@asyncio.coroutine
def pipeline(pool, val, loop):
    val = 'val:{}'.format(val)
    with (yield from pool) as redis:
        f1 = redis.set('key', val)
        f2 = redis.get('key', encoding='utf-8')
        ok, res = yield from asyncio.gather(f1, f2, loop=loop)


@asyncio.coroutine
def transaction(pool, val, loop):
    val = 'val:{}'.format(val)
    with (yield from pool) as redis:
        tr = redis.multi_exec()
        tr.set('key', val)
        tr.get('key', encoding='utf-8')
        ok, res = yield from tr.execute()
        assert ok, ok
        assert res == val


@asyncio.coroutine
def blocking_pop(pool, val, loop):

    @asyncio.coroutine
    def lpush():
        with (yield from pool) as redis:
            # here v0.3 has bound connection, v1.0 does not;
            yield from asyncio.sleep(.1, loop=loop)
            yield from redis.lpush('list-key', 'val')

    @asyncio.coroutine
    def blpop():
        with (yield from pool) as redis:
            # here v0.3 has bound connection, v1.0 does not;
            res = yield from redis.blpop(
                'list-key', timeout=1, encoding='utf-8')
            assert res == ['list-key', 'val'], res
    yield from asyncio.gather(blpop(), lpush(), loop=loop)


@pytest.mark.run_loop
@pytest.mark.parametrize('test_case,pool_size', [
    (simple_get_set, 1),
    (pipeline, 1),
    (transaction, 1),
    pytest.mark.xfail((blocking_pop, 1),
                      reason="blpop gets connection first and blocks"),
    (simple_get_set, 10),
    (pipeline, 10),
    (transaction, 10),
    (blocking_pop, 10),
], ids=lambda o: o.__name__)
def test_operations(pool_or_redis, test_case, pool_size, loop):
    repeat = 100
    redis = yield from pool_or_redis(pool_size)
    done, pending = yield from asyncio.wait(
        [async_task(test_case(redis, i, loop), loop=loop)
         for i in range(repeat)], loop=loop)

    assert not pending
    success = 0
    failures = []
    for fut in done:
        exc = fut.exception()
        if exc is None:
            success += 1
        else:
            failures.append(exc)
    assert repeat == success, failures
    assert not failures
