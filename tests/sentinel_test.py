import asyncio
import pytest
from unittest import mock

import aioredis.errors


"""
This was tested using https://github.com/denverdino/redis-cluster .

It had one local diff:

--- a/redis-config/config.sh
+++ b/redis-config/config.sh
@@ -6,2 +6,4 @@ echo $redismaster_ip

+redis-cli -h $redisslave_ip -p 6379  config set slave-read-only yes
+redis-cli -h $redismaster_ip -p 6379  config set slave-read-only yes
 redis-cli -h $redisslave_ip -p 6379 slaveof $redismaster_ip 6379

"""

master_name = "STUB"
get_master_connection = mock.Mock()
get_slave_connection = mock.Mock()
redis_sentinel = mock.Mock()


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.mark.run_loop
def test_sentinel_normal():
    key, field, value = b'key:hset', b'bar', b'zap'
    redis = yield from get_master_connection()
    exists = yield from redis.hexists(key, field)
    if exists:
        ret = yield from redis.hdel(key, field)
        assert ret != 1

    ret = yield from redis.hset(key, field, value)
    assert ret == 1
    ret = yield from redis.hset(key, field, value)
    assert ret == 0


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.mark.run_loop
def test_sentinel_slave():
    key, field, value = b'key:hset', b'bar', b'zap'
    redis = yield from get_slave_connection()
    exists = yield from redis.hexists(key, field)
    if exists:
        with pytest.raises(aioredis.errors.ReadOnlyError):
            yield from redis.hdel(key, field)

    with pytest.raises(aioredis.errors.ReadOnlyError):
        yield from redis.hset(key, field, value)


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.mark.run_loop       # (timeout=600)
def test_sentinel_slave_fail(loop):
    sentinel_connection = redis_sentinel.get_sentinel_connection(0)
    key, field, value = b'key:hset', b'bar', b'zap'
    redis = yield from get_slave_connection()
    exists = yield from redis.hexists(key, field)
    if exists:
        with pytest.raises(aioredis.errors.ReadOnlyError):
            yield from redis.hdel(key, field)

    with pytest.raises(aioredis.errors.ReadOnlyError):
        yield from redis.hset(key, field, value)

    ret = yield from sentinel_connection.sentinel_failover(master_name)
    assert ret is True
    yield from asyncio.sleep(2, loop=loop)

    with pytest.raises(aioredis.errors.ReadOnlyError):
        yield from redis.hset(key, field, value)

    ret = yield from sentinel_connection.sentinel_failover(master_name)
    assert ret is True
    yield from asyncio.sleep(2, loop=loop)
    redis = yield from get_slave_connection()
    while True:
        try:
            yield from redis.hset(key, field, value)
            yield from asyncio.sleep(1, loop=loop)
            redis = yield from get_slave_connection()
        except aioredis.errors.ReadOnlyError:
            break


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.mark.run_loop
def test_sentinel_normal_fail(loop):
    sentinel_connection = redis_sentinel.get_sentinel_connection(0)
    key, field, value = b'key:hset', b'bar', b'zap'
    redis = yield from get_master_connection()
    exists = yield from redis.hexists(key, field)
    if exists:
        ret = yield from redis.hdel(key, field)
        assert ret == 1

    ret = yield from redis.hset(key, field, value)
    assert ret == 1
    ret = yield from sentinel_connection.sentinel_failover(master_name)
    assert ret is True
    yield from asyncio.sleep(2, loop=loop)
    ret = yield from redis.hset(key, field, value)
    assert ret == 0
    ret = yield from sentinel_connection.sentinel_failover(master_name)
    assert ret is True
    yield from asyncio.sleep(2, loop=loop)
    redis = yield from get_slave_connection()
    while True:
        try:
            yield from redis.hset(key, field, value)
            yield from asyncio.sleep(1, loop=loop)
            redis = yield from get_slave_connection()
        except aioredis.errors.ReadOnlyError:
            break


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.mark.run_loop
def test_failover(loop):
    sentinel_connection = redis_sentinel.get_sentinel_connection(0)
    func = sentinel_connection.sentinel_get_master_addr_by_name
    orig_master = yield from func(master_name)
    ret = yield from sentinel_connection.sentinel_failover(master_name)
    assert ret is True
    yield from asyncio.sleep(2, loop=loop)
    new_master = yield from func(master_name)
    assert orig_master != new_master
    ret = yield from sentinel_connection.sentinel_failover(master_name)
    assert ret is True
    yield from asyncio.sleep(2, loop=loop)
    new_master = yield from func(master_name)
    assert orig_master == new_master
    redis = yield from get_slave_connection()
    key, field, value = b'key:hset', b'bar', b'zap'
    while True:
        try:
            yield from redis.hset(key, field, value)
            yield from asyncio.sleep(1, loop=loop)
            redis = yield from get_slave_connection()
        except aioredis.errors.ReadOnlyError:
            break


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.mark.run_loop
def test_get_master():
    sentinel_connection = redis_sentinel.get_sentinel_connection(0)
    func = sentinel_connection.sentinel_get_master_addr_by_name
    master = yield from func(master_name)
    assert isinstance(master, tuple)
    assert len(master) == 2
    assert master[1] == 6379


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.mark.run_loop
def test_get_masters():
    sentinel_connection = redis_sentinel.get_sentinel_connection(0)
    master = yield from sentinel_connection.sentinel_masters()
    assert isinstance(master, dict)
    assert master_name in master
    master = master[master_name]
    assert master['is_slave'] is False
    assert master['name'] == master_name
    for k in ['is_master_down', 'num-other-sentinels', 'flags', 'is_odown',
              'quorum', 'ip', 'failover-timeout', 'runid', 'info-refresh',
              'config-epoch', 'parallel-syncs', 'role-reported-time',
              'is_sentinel', 'last-ok-ping-reply',
              'last-ping-reply', 'last-ping-sent', 'is_sdown', 'is_master',
              'name', 'pending-commands', 'down-after-milliseconds',
              'is_slave', 'num-slaves', 'port', 'is_disconnected',
              'role-reported']:
        assert k in master


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.mark.run_loop
def test_get_master_info():
    sentinel_connection = redis_sentinel.get_sentinel_connection(0)
    master = yield from sentinel_connection.sentinel_master(master_name)
    assert isinstance(master, dict)
    assert master['is_slave'] is False
    assert master['name'] == master_name
    for k in ['is_master_down', 'num-other-sentinels', 'flags', 'is_odown',
              'quorum', 'ip', 'failover-timeout', 'runid', 'info-refresh',
              'config-epoch', 'parallel-syncs', 'role-reported-time',
              'is_sentinel', 'last-ok-ping-reply',
              'last-ping-reply', 'last-ping-sent', 'is_sdown', 'is_master',
              'name', 'pending-commands', 'down-after-milliseconds',
              'is_slave', 'num-slaves', 'port', 'is_disconnected',
              'role-reported']:
        assert k in master


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.mark.run_loop
def test_get_slave_info():
    sentinel_connection = redis_sentinel.get_sentinel_connection(0)
    slave = yield from sentinel_connection.sentinel_slaves(master_name)
    assert len(slave) == 1
    slave = slave[0]
    assert isinstance(slave, dict)
    assert slave['is_slave'] is True
    for k in ['is_master_down', 'flags', 'is_odown',
              'ip', 'runid', 'info-refresh',
              'role-reported-time',
              'is_sentinel', 'last-ok-ping-reply',
              'last-ping-reply', 'last-ping-sent', 'is_sdown', 'is_master',
              'name', 'pending-commands', 'down-after-milliseconds',
              'is_slave', 'port', 'is_disconnected', 'role-reported']:
        assert k in slave, k


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.mark.run_loop
def test_get_sentinel_info():
    sentinel_connection = redis_sentinel.get_sentinel_connection(0)
    sentinel = yield from sentinel_connection.sentinel_sentinels(master_name)
    assert len(sentinel) == 0


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.mark.run_loop
def test_get_sentinel_set_error():
    sentinel_connection = redis_sentinel.get_sentinel_connection(0)
    with pytest.raises(aioredis.errors.ReplyError):
        yield from sentinel_connection.sentinel_set(master_name, 'foo', 'bar')


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.mark.run_loop
def test_get_sentinel_set():
    sentinel_connection = redis_sentinel.get_sentinel_connection(0)
    resp = yield from sentinel_connection.sentinel_set(
        master_name, 'failover-timeout', 1100)
    assert resp is True
    master = yield from sentinel_connection.sentinel_masters()
    assert master[master_name]['failover-timeout'] == 1100


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.mark.run_loop
def test_get_sentinel_monitor():
    sentinel_connection = redis_sentinel.get_sentinel_connection(0)
    master = yield from sentinel_connection.sentinel_masters()
    if len(master):
        if 'mymaster2' in master:
            resp = yield from sentinel_connection.sentinel_remove('mymaster2')
            assert resp is True
    resp = yield from sentinel_connection.sentinel_monitor('mymaster2',
                                                           '127.0.0.1',
                                                           6380, 2)
    assert resp is True
    resp = yield from sentinel_connection.sentinel_remove('mymaster2')
    assert resp is True
