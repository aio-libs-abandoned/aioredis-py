import pytest
import asyncio
import sys

from aioredis import (
    SlaveNotFoundError,
    ReadOnlyError,
    )


pytestmark = pytest.redis_version(2, 8, 12, reason="Sentinel v2 required")
if sys.platform == 'win32':
    pytestmark = pytest.mark.skip(reason="unstable on windows")


@pytest.mark.xfail
@pytest.mark.run_loop(timeout=40)
def test_auto_failover(start_sentinel, start_server,
                       create_sentinel, create_connection, loop):
    server1 = start_server('master-failover', ['slave-read-only yes'])
    start_server('slave-failover1', ['slave-read-only yes'], slaveof=server1)
    start_server('slave-failover2', ['slave-read-only yes'], slaveof=server1)

    sentinel1 = start_sentinel('sentinel-failover1', server1, quorum=2)
    sentinel2 = start_sentinel('sentinel-failover2', server1, quorum=2)

    sp = yield from create_sentinel([sentinel1.tcp_address,
                                     sentinel2.tcp_address])

    _, old_port = yield from sp.master_address(server1.name)
    # ignoring host
    assert old_port == server1.tcp_address.port
    master = sp.master_for(server1.name)
    res = yield from master.role()
    assert res.role == 'master'
    assert master.address is not None
    assert master.address[1] == old_port

    # wait failover
    conn = yield from create_connection(server1.tcp_address)
    yield from conn.execute("debug", "sleep", 6)
    yield from asyncio.sleep(3, loop=loop)

    # _, new_port = yield from sp.master_address(server1.name)
    # assert new_port != old_port
    # assert new_port == server2.tcp_address.port
    assert (yield from master.set("key", "val"))
    assert master.address is not None
    assert master.address[1] != old_port


@pytest.mark.run_loop
def test_sentinel_normal(sentinel, create_sentinel):
    redis_sentinel = yield from create_sentinel([sentinel.tcp_address])
    redis = redis_sentinel.master_for('masterA')

    info = yield from redis.role()
    assert info.role == 'master'

    key, field, value = b'key:hset', b'bar', b'zap'
    exists = yield from redis.hexists(key, field)
    if exists:
        ret = yield from redis.hdel(key, field)
        assert ret != 1

    ret = yield from redis.hset(key, field, value)
    assert ret == 1
    ret = yield from redis.hset(key, field, value)
    assert ret == 0


@pytest.mark.xfail(reason="same sentinel; single master;")
@pytest.mark.run_loop
def test_sentinel_slave(sentinel, create_sentinel):
    redis_sentinel = yield from create_sentinel([sentinel.tcp_address])
    redis = redis_sentinel.slave_for('masterA')

    info = yield from redis.role()
    assert info.role == 'slave'

    key, field, value = b'key:hset', b'bar', b'zap'
    # redis = yield from get_slave_connection()
    exists = yield from redis.hexists(key, field)
    if exists:
        with pytest.raises(ReadOnlyError):
            yield from redis.hdel(key, field)

    with pytest.raises(ReadOnlyError):
        yield from redis.hset(key, field, value)


@pytest.mark.xfail(reason="Need proper sentinel configuration")
@pytest.mark.run_loop       # (timeout=600)
def test_sentinel_slave_fail(sentinel, create_sentinel, loop):
    redis_sentinel = yield from create_sentinel([sentinel.tcp_address])

    key, field, value = b'key:hset', b'bar', b'zap'

    redis = redis_sentinel.slave_for('masterA')
    exists = yield from redis.hexists(key, field)
    if exists:
        with pytest.raises(ReadOnlyError):
            yield from redis.hdel(key, field)

    with pytest.raises(ReadOnlyError):
        yield from redis.hset(key, field, value)

    ret = yield from redis_sentinel.failover('masterA')
    assert ret is True
    yield from asyncio.sleep(2, loop=loop)

    with pytest.raises(ReadOnlyError):
        yield from redis.hset(key, field, value)

    ret = yield from redis_sentinel.failover('masterA')
    assert ret is True
    yield from asyncio.sleep(2, loop=loop)
    while True:
        try:
            yield from asyncio.sleep(1, loop=loop)
            yield from redis.hset(key, field, value)
        except SlaveNotFoundError:
            continue
        except ReadOnlyError:
            break


@pytest.mark.xfail(reason="Need proper sentinel configuration")
@pytest.mark.run_loop
def test_sentinel_normal_fail(sentinel, create_sentinel, loop):
    redis_sentinel = yield from create_sentinel([sentinel.tcp_address])

    key, field, value = b'key:hset', b'bar', b'zap'
    redis = redis_sentinel.master_for('masterA')
    exists = yield from redis.hexists(key, field)
    if exists:
        ret = yield from redis.hdel(key, field)
        assert ret == 1

    ret = yield from redis.hset(key, field, value)
    assert ret == 1
    ret = yield from redis_sentinel.failover('masterA')
    assert ret is True
    yield from asyncio.sleep(2, loop=loop)
    ret = yield from redis.hset(key, field, value)
    assert ret == 0
    ret = yield from redis_sentinel.failover('masterA')
    assert ret is True
    yield from asyncio.sleep(2, loop=loop)
    redis = redis_sentinel.slave_for('masterA')
    while True:
        try:
            yield from redis.hset(key, field, value)
            yield from asyncio.sleep(1, loop=loop)
            # redis = yield from get_slave_connection()
        except ReadOnlyError:
            break


@pytest.mark.xfail(reason="same sentinel; single master;")
@pytest.mark.run_loop
def test_failover_command(sentinel, create_sentinel, loop):
    master_name = 'masterA'
    redis_sentinel = yield from create_sentinel([sentinel.tcp_address])

    orig_master = yield from redis_sentinel.master_address(master_name)
    ret = yield from redis_sentinel.failover(master_name)
    assert ret is True
    yield from asyncio.sleep(2, loop=loop)

    new_master = yield from redis_sentinel.master_address(master_name)
    assert orig_master != new_master

    ret = yield from redis_sentinel.failover(master_name)
    assert ret is True
    yield from asyncio.sleep(2, loop=loop)

    new_master = yield from redis_sentinel.master_address(master_name)
    assert orig_master == new_master

    redis = redis_sentinel.slave_for(master_name)
    key, field, value = b'key:hset', b'bar', b'zap'
    while True:
        try:
            yield from asyncio.sleep(1, loop=loop)
            yield from redis.hset(key, field, value)
        except SlaveNotFoundError:
            pass
        except ReadOnlyError:
            break
