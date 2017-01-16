import pytest
import asyncio

from unittest import mock

from aioredis import (
    SlaveNotFoundError,
    ReadOnlyError,
    ReplyError,
    )


pytestmark = pytest.redis_version(2, 8, 12, reason="Sentinel v2 required")


@pytest.mark.run_loop
def test_sentinel2(sentinel, create_sentinel):
    sp = yield from create_sentinel([sentinel.tcp_address])
    master = sp.master_for('masterA')
    # assert isinstance(master, pool.Redis)
    res = yield from master.set('key', 'val')
    assert res is True
    res = yield from master.get('key')
    assert res == b'val'


@pytest.mark.run_loop(timeout=40)
def test_failover2(start_sentinel, start_server,
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
    address = master.address
    assert address is not None
    _, old_port = address

    # wait failover
    conn = yield from create_connection(server1.tcp_address)
    yield from conn.execute("debug", "sleep", 6)

    # _, new_port = yield from sp.master_address(server1.name)
    # assert new_port != old_port
    # assert new_port == server2.tcp_address.port
    res = yield from master.set("key", "val")
    assert res
    assert master.address is not None
    assert master.address[1] != old_port


@pytest.mark.run_loop
def test_sentinel_role(sentinel, create_redis, loop):
    redis = yield from create_redis(sentinel.tcp_address, loop=loop)
    info = yield from redis.role()
    assert info.role == 'sentinel'
    assert info.masters == ['masterA']


@pytest.mark.run_loop
def test_sentinel_masters(sentinel, create_sentinel):
    redis_sentinel = yield from create_sentinel([sentinel.tcp_address])

    info = {
        'config-epoch': 0,
        'down-after-milliseconds': 3000,
        'failover-timeout': 3000,
        'flags': frozenset(('master',)),
        'info-refresh': mock.ANY,
        'ip': '127.0.0.1',
        'last-ok-ping-reply': mock.ANY,
        'last-ping-reply': mock.ANY,
        'last-ping-sent': mock.ANY,
        'name': 'masterA',
        'num-other-sentinels': 0,
        'num-slaves': 1,
        'parallel-syncs': 1,
        'port': sentinel.masters['masterA'].tcp_address.port,
        'quorum': 1,
        'role-reported': 'master',
        'role-reported-time': mock.ANY,
        'runid': mock.ANY,
        }
    if sentinel.version < (3, 2, 0):
        info['pending-commands'] = 0
    else:
        info['link-pending-commands'] = 0
        info['link-refcount'] = mock.ANY

    res = yield from redis_sentinel.masters()
    assert res == {'masterA': info}


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
def test_failover(sentinel, create_sentinel, loop):
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


@pytest.mark.xfail(reason="Need proper sentinel configuration")
@pytest.mark.run_loop
def test_master_for(sentinel, create_sentinel):
    redis_sentinel = yield from create_sentinel([sentinel.tcp_address])
    master = yield from redis_sentinel.master_address('masterA')
    assert isinstance(master, tuple)
    assert len(master) == 2
    assert master[0] == '127.0.0.1'
    # after several above tests master is unknown
    assert master[1] == sentinel.masters['masterA'].tcp_address.port


@pytest.mark.run_loop
def test_get_master_info(sentinel, create_sentinel):
    redis_sentinel = yield from create_sentinel([sentinel.tcp_address])

    info = yield from redis_sentinel.master('masterA')
    assert isinstance(info, dict)
    assert info['name'] == 'masterA'
    assert 'slave' not in info['flags']
    assert 's_down' not in info['flags']
    assert 'o_down' not in info['flags']
    assert 'sentinel' not in info['flags']
    assert 'disconnected' not in info['flags']
    assert 'master' in info['flags']

    for key in ['num-other-sentinels',
                'flags',
                'quorum',
                'ip',
                'failover-timeout',
                'runid',
                'info-refresh',
                'config-epoch',
                'parallel-syncs',
                'role-reported-time',
                'last-ok-ping-reply',
                'last-ping-reply',
                'last-ping-sent',
                'name',
                'down-after-milliseconds',
                'num-slaves',
                'port',
                'role-reported']:
        assert key in info
    if sentinel.version < (3, 2, 0):
        assert 'pending-commands' in info
    else:
        assert 'link-pending-commands' in info
        assert 'link-refcount' in info


@pytest.mark.run_loop
def test_get_slave_info(sentinel, create_sentinel):
    redis_sentinel = yield from create_sentinel([sentinel.tcp_address])

    info = yield from redis_sentinel.slaves('masterA')
    assert len(info) == 1
    info = info[0]
    assert isinstance(info, dict)
    assert 'master' not in info['flags']
    assert 's_down' not in info['flags']
    assert 'o_down' not in info['flags']
    assert 'sentinel' not in info['flags']
    # assert 'disconnected' not in info['flags']
    assert 'slave' in info['flags']

    keys_set = {
        'flags',
        'master-host',
        'master-link-down-time',
        'master-link-status',
        'master-port',
        'name',
        'slave-priority',
        'ip',
        'runid',
        'info-refresh',
        'role-reported-time',
        'last-ok-ping-reply',
        'last-ping-reply',
        'last-ping-sent',
        'down-after-milliseconds',
        'port',
        'role-reported',
    }
    if sentinel.version < (3, 2, 0):
        keys_set.add('pending-commands')
    else:
        keys_set.add('link-pending-commands')
        keys_set.add('link-refcount')

    missing = keys_set - set(info)
    assert not missing
    # extra = set(info) - keys_set
    # assert not extra


@pytest.mark.run_loop
def test_sentinels(sentinel, create_sentinel,
                   start_sentinel, start_server, loop):
    redis_sentinel = yield from create_sentinel([sentinel.tcp_address])

    # no other sentinels
    info = yield from redis_sentinel.sentinels('masterA')
    assert len(info) == 0

    with pytest.raises(ReplyError):
        yield from redis_sentinel.sentinels('bad_master')

    # another sentinel
    sentinel2 = start_sentinel('sentinelB', start_server('masterA'))
    yield from asyncio.sleep(3, loop=loop)

    info = yield from redis_sentinel.sentinels('masterA')
    assert len(info) == 1
    assert 'sentinel' in info[0]['flags']
    assert info[0]['port'] == sentinel2.tcp_address.port

    redis_sentinel2 = yield from create_sentinel([sentinel2.tcp_address])
    yield from redis_sentinel2.remove('masterA')


@pytest.mark.run_loop
def test_get_sentinel_set_error(sentinel, create_sentinel):
    redis_sentinel = yield from create_sentinel([sentinel.tcp_address])
    with pytest.raises(ReplyError):
        yield from redis_sentinel.set('masterA', 'foo', 'bar')


@pytest.mark.run_loop
def test_get_sentinel_set(sentinel, create_sentinel):
    redis_sentinel = yield from create_sentinel([sentinel.tcp_address])
    resp = yield from redis_sentinel.set('masterA', 'failover-timeout', 1100)
    assert resp is True
    master = yield from redis_sentinel.masters()
    assert master['masterA']['failover-timeout'] == 1100


@pytest.mark.run_loop
def test_sentinel_monitor(sentinel, create_sentinel, start_server):
    redis_sentinel = yield from create_sentinel([sentinel.tcp_address])

    masters = yield from redis_sentinel.masters()
    assert len(masters) == 1
    assert 'masterA' in masters

    masterB = start_server('masterB')

    ok = yield from redis_sentinel.monitor('masterB', '127.0.0.1',
                                           masterB.tcp_address.port, 2)
    assert ok is True

    masters = yield from redis_sentinel.masters()
    assert len(masters) == 2
    assert 'masterA' in masters
    assert 'masterB' in masters

    ok = yield from redis_sentinel.remove('masterB')
    assert ok is True

    masters = yield from redis_sentinel.masters()
    assert len(masters) == 1
    assert 'masterA' in masters


@pytest.mark.run_loop
def test_sentinel_ckquorum(sentinel, create_sentinel):
    redis_sentinel = yield from create_sentinel([sentinel.tcp_address])
    ok = yield from redis_sentinel.check_quorum('masterA')
    assert ok
