import pytest
import asyncio

from aioredis import ConnectionClosedError, ReplyError
from aioredis.pool import ConnectionsPool
from aioredis import Redis
from _testutils import redis_version


async def test_repr(create_redis, server):
    redis = await create_redis(server.tcp_address, db=1)
    assert repr(redis) in {
        '<Redis <RedisConnection [db:1]>>',
        '<Redis <ConnectionsPool [db:1, size:[1:10], free:1]>>',
        }

    redis = await create_redis(server.tcp_address, db=0)
    assert repr(redis) in {
        '<Redis <RedisConnection [db:0]>>',
        '<Redis <ConnectionsPool [db:0, size:[1:10], free:1]>>',
        }


async def test_auth(redis):
    expected_message = "ERR Client sent AUTH, but no password is set"
    with pytest.raises(ReplyError, match=expected_message):
        await redis.auth('')


async def test_echo(redis):
    resp = await redis.echo('ECHO')
    assert resp == b'ECHO'

    with pytest.raises(TypeError):
        await redis.echo(None)


async def test_ping(redis):
    assert await redis.ping() == b'PONG'


async def test_quit(redis):
    expected = (ConnectionClosedError, ConnectionError)
    try:
        assert b'OK' == await redis.quit()
    except expected:
        pass

    if not isinstance(redis.connection, ConnectionsPool):
        # reader task may not yet been cancelled and _do_close not called
        #   so the ConnectionClosedError may be raised (or ConnectionError)
        with pytest.raises(expected):
            try:
                await redis.ping()
            except asyncio.CancelledError:
                assert False, "Cancelled error must not be raised"

        # wait one loop iteration until it get surely closed
        await asyncio.sleep(0)
        assert redis.connection.closed

        with pytest.raises(ConnectionClosedError):
            await redis.ping()


async def test_select(redis):
    assert redis.db == 0

    resp = await redis.select(1)
    assert resp is True
    assert redis.db == 1
    assert redis.connection.db == 1


async def test_encoding(create_redis, server):
    redis = await create_redis(server.tcp_address, db=1, encoding='utf-8')
    assert redis.encoding == 'utf-8'


async def test_yield_from_backwards_compatibility(create_redis, server):
    redis = await create_redis(server.tcp_address)

    assert isinstance(redis, Redis)
    # TODO: there should not be warning
    # with pytest.warns(UserWarning):
    with await redis as client:
        assert isinstance(client, Redis)
        assert client is not redis
        assert await client.ping()


@redis_version(4, 0, 0, reason="SWAPDB is available since redis>=4.0.0")
async def test_swapdb(create_redis, start_server):
    server = start_server('swapdb_1')
    cli1 = await create_redis(server.tcp_address, db=0)
    cli2 = await create_redis(server.tcp_address, db=1)

    await cli1.flushall()
    assert await cli1.set('key', 'val') is True
    assert await cli1.exists('key')
    assert not await cli2.exists('key')

    assert await cli1.swapdb(0, 1) is True
    assert not await cli1.exists('key')
    assert await cli2.exists('key')
