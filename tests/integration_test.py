import asyncio
import pytest

import aioredis


@pytest.fixture
def pool_or_redis(_closable, server):
    version = tuple(map(int, aioredis.__version__.split('.')[:2]))
    if version >= (1, 0):
        factory = aioredis.create_redis_pool
    else:
        factory = aioredis.create_pool

    async def redis_factory(maxsize):
        redis = await factory(server.tcp_address,
                              minsize=1, maxsize=maxsize)
        _closable(redis)
        return redis
    return redis_factory


async def simple_get_set(pool, idx):
    """A simple test to make sure Redis(pool) can be used as old Pool(Redis).
    """
    val = 'val:{}'.format(idx)
    with await pool as redis:
        assert await redis.set('key', val)
        await redis.get('key', encoding='utf-8')


async def pipeline(pool, val):
    val = 'val:{}'.format(val)
    with await pool as redis:
        f1 = redis.set('key', val)
        f2 = redis.get('key', encoding='utf-8')
        ok, res = await asyncio.gather(f1, f2)


async def transaction(pool, val):
    val = 'val:{}'.format(val)
    with await pool as redis:
        tr = redis.multi_exec()
        tr.set('key', val)
        tr.get('key', encoding='utf-8')
        ok, res = await tr.execute()
        assert ok, ok
        assert res == val


async def blocking_pop(pool, val):

    async def lpush():
        with await pool as redis:
            # here v0.3 has bound connection, v1.0 does not;
            await asyncio.sleep(.1)
            await redis.lpush('list-key', 'val')

    async def blpop():
        with await pool as redis:
            # here v0.3 has bound connection, v1.0 does not;
            res = await redis.blpop(
                'list-key', timeout=2, encoding='utf-8')
            assert res == ['list-key', 'val'], res
    await asyncio.gather(blpop(), lpush())


@pytest.mark.parametrize('test_case,pool_size', [
    (simple_get_set, 1),
    (pipeline, 1),
    (transaction, 1),
    pytest.param(
        blocking_pop, 1,
        marks=pytest.mark.xfail(
            reason="blpop gets connection first and blocks")
        ),
    (simple_get_set, 10),
    (pipeline, 10),
    (transaction, 10),
    (blocking_pop, 10),
], ids=lambda o: getattr(o, '__name__', repr(o)))
async def test_operations(pool_or_redis, test_case, pool_size):
    repeat = 100
    redis = await pool_or_redis(pool_size)
    done, pending = await asyncio.wait(
        [asyncio.ensure_future(test_case(redis, i))
         for i in range(repeat)])

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
