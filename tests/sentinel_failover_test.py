import pytest
import asyncio
import sys

from aioredis import (
    SlaveNotFoundError,
    ReadOnlyError,
    )
from _testutils import redis_version


pytestmark = redis_version(2, 8, 12, reason="Sentinel v2 required")
if sys.platform == 'win32':
    pytestmark = pytest.mark.skip(reason="unstable on windows")


@pytest.mark.xfail
@pytest.mark.run_loop(timeout=40)
async def test_auto_failover(start_sentinel, start_server,
                             create_sentinel, create_connection, loop):
    server1 = start_server('master-failover', ['slave-read-only yes'])
    start_server('slave-failover1', ['slave-read-only yes'], slaveof=server1)
    start_server('slave-failover2', ['slave-read-only yes'], slaveof=server1)

    sentinel1 = start_sentinel('sentinel-failover1', server1, quorum=2)
    sentinel2 = start_sentinel('sentinel-failover2', server1, quorum=2)

    sp = await create_sentinel([sentinel1.tcp_address,
                                sentinel2.tcp_address],
                               timeout=1)

    _, old_port = await sp.master_address(server1.name)
    # ignoring host
    assert old_port == server1.tcp_address.port
    master = sp.master_for(server1.name)
    res = await master.role()
    assert res.role == 'master'
    assert master.address is not None
    assert master.address[1] == old_port

    # wait failover
    conn = await create_connection(server1.tcp_address)
    await conn.execute("debug", "sleep", 6)
    await asyncio.sleep(3, loop=loop)

    # _, new_port = await sp.master_address(server1.name)
    # assert new_port != old_port
    # assert new_port == server2.tcp_address.port
    assert (await master.set("key", "val"))
    assert master.address is not None
    assert master.address[1] != old_port


@pytest.mark.run_loop
async def test_sentinel_normal(sentinel, create_sentinel):
    redis_sentinel = await create_sentinel([sentinel.tcp_address], timeout=1)
    redis = redis_sentinel.master_for('masterA')

    info = await redis.role()
    assert info.role == 'master'

    key, field, value = b'key:hset', b'bar', b'zap'
    exists = await redis.hexists(key, field)
    if exists:
        ret = await redis.hdel(key, field)
        assert ret != 1

    ret = await redis.hset(key, field, value)
    assert ret == 1
    ret = await redis.hset(key, field, value)
    assert ret == 0


@pytest.mark.xfail(reason="same sentinel; single master;")
@pytest.mark.run_loop
async def test_sentinel_slave(sentinel, create_sentinel):
    redis_sentinel = await create_sentinel([sentinel.tcp_address], timeout=1)
    redis = redis_sentinel.slave_for('masterA')

    info = await redis.role()
    assert info.role == 'slave'

    key, field, value = b'key:hset', b'bar', b'zap'
    # redis = await get_slave_connection()
    exists = await redis.hexists(key, field)
    if exists:
        with pytest.raises(ReadOnlyError):
            await redis.hdel(key, field)

    with pytest.raises(ReadOnlyError):
        await redis.hset(key, field, value)


@pytest.mark.xfail(reason="Need proper sentinel configuration")
@pytest.mark.run_loop       # (timeout=600)
async def test_sentinel_slave_fail(sentinel, create_sentinel, loop):
    redis_sentinel = await create_sentinel([sentinel.tcp_address], timeout=1)

    key, field, value = b'key:hset', b'bar', b'zap'

    redis = redis_sentinel.slave_for('masterA')
    exists = await redis.hexists(key, field)
    if exists:
        with pytest.raises(ReadOnlyError):
            await redis.hdel(key, field)

    with pytest.raises(ReadOnlyError):
        await redis.hset(key, field, value)

    ret = await redis_sentinel.failover('masterA')
    assert ret is True
    await asyncio.sleep(2, loop=loop)

    with pytest.raises(ReadOnlyError):
        await redis.hset(key, field, value)

    ret = await redis_sentinel.failover('masterA')
    assert ret is True
    await asyncio.sleep(2, loop=loop)
    while True:
        try:
            await asyncio.sleep(1, loop=loop)
            await redis.hset(key, field, value)
        except SlaveNotFoundError:
            continue
        except ReadOnlyError:
            break


@pytest.mark.xfail(reason="Need proper sentinel configuration")
@pytest.mark.run_loop
async def test_sentinel_normal_fail(sentinel, create_sentinel, loop):
    redis_sentinel = await create_sentinel([sentinel.tcp_address], timeout=1)

    key, field, value = b'key:hset', b'bar', b'zap'
    redis = redis_sentinel.master_for('masterA')
    exists = await redis.hexists(key, field)
    if exists:
        ret = await redis.hdel(key, field)
        assert ret == 1

    ret = await redis.hset(key, field, value)
    assert ret == 1
    ret = await redis_sentinel.failover('masterA')
    assert ret is True
    await asyncio.sleep(2, loop=loop)
    ret = await redis.hset(key, field, value)
    assert ret == 0
    ret = await redis_sentinel.failover('masterA')
    assert ret is True
    await asyncio.sleep(2, loop=loop)
    redis = redis_sentinel.slave_for('masterA')
    while True:
        try:
            await redis.hset(key, field, value)
            await asyncio.sleep(1, loop=loop)
            # redis = await get_slave_connection()
        except ReadOnlyError:
            break


@pytest.mark.xfail(reason="same sentinel; single master;")
@pytest.mark.run_loop
async def test_failover_command(sentinel, create_sentinel, loop):
    master_name = 'masterA'
    redis_sentinel = await create_sentinel([sentinel.tcp_address], timeout=1)

    orig_master = await redis_sentinel.master_address(master_name)
    ret = await redis_sentinel.failover(master_name)
    assert ret is True
    await asyncio.sleep(2, loop=loop)

    new_master = await redis_sentinel.master_address(master_name)
    assert orig_master != new_master

    ret = await redis_sentinel.failover(master_name)
    assert ret is True
    await asyncio.sleep(2, loop=loop)

    new_master = await redis_sentinel.master_address(master_name)
    assert orig_master == new_master

    redis = redis_sentinel.slave_for(master_name)
    key, field, value = b'key:hset', b'bar', b'zap'
    while True:
        try:
            await asyncio.sleep(1, loop=loop)
            await redis.hset(key, field, value)
        except SlaveNotFoundError:
            pass
        except ReadOnlyError:
            break
