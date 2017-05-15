import asyncio
import pytest
import sys

from aioredis import RedisError, ReplyError, PoolClosedError
from aioredis.sentinel.commands import RedisSentinel

pytestmark = pytest.redis_version(2, 8, 12, reason="Sentinel v2 required")
if sys.platform == 'win32':
    pytestmark = pytest.mark.skip(reason="unstable on windows")


@pytest.mark.run_loop
def test_client_close(redis_sentinel):
    assert isinstance(redis_sentinel, RedisSentinel)
    assert not redis_sentinel.closed

    redis_sentinel.close()
    assert redis_sentinel.closed
    with pytest.raises(PoolClosedError):
        assert (yield from redis_sentinel.ping()) != b'PONG'

    yield from redis_sentinel.wait_closed()


@pytest.mark.run_loop
def test_global_loop(sentinel, create_sentinel, loop):
    asyncio.set_event_loop(loop)

    # force global loop
    client = yield from create_sentinel([sentinel.tcp_address], loop=None)
    assert client._pool._loop is loop

    asyncio.set_event_loop(None)


@pytest.mark.run_loop
def test_ping(redis_sentinel):
    assert b'PONG' == (yield from redis_sentinel.ping())


@pytest.mark.run_loop
def test_master_info(redis_sentinel, sentinel):
    info = yield from redis_sentinel.master('master-no-fail')
    assert isinstance(info, dict)
    assert info['name'] == 'master-no-fail'
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
def test_master__unknown(redis_sentinel):
    with pytest.raises(ReplyError):
        yield from redis_sentinel.master('unknown-master')


@pytest.mark.run_loop
def test_master_address(redis_sentinel, sentinel):
    _, port = yield from redis_sentinel.master_address('master-no-fail')
    assert port == sentinel.masters['master-no-fail'].tcp_address.port


@pytest.mark.run_loop
def test_master_address__unknown(redis_sentinel):
    res = yield from redis_sentinel.master_address('unknown-master')
    assert res is None


@pytest.mark.run_loop
def test_masters(redis_sentinel):
    masters = yield from redis_sentinel.masters()
    assert isinstance(masters, dict)
    assert len(masters) >= 1, "At least on masters expected"
    assert 'master-no-fail' in masters
    assert isinstance(masters['master-no-fail'], dict)


@pytest.mark.run_loop
def test_slave_info(sentinel, redis_sentinel):
    info = yield from redis_sentinel.slaves('master-no-fail')
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


@pytest.mark.run_loop
def test_slave__unknown(redis_sentinel):
    with pytest.raises(ReplyError):
        yield from redis_sentinel.slaves('unknown-master')


@pytest.mark.run_loop
def test_sentinels_empty(redis_sentinel):
    res = yield from redis_sentinel.sentinels('master-no-fail')
    assert res == []

    with pytest.raises(ReplyError):
        yield from redis_sentinel.sentinels('unknown-master')


@pytest.mark.run_loop(timeout=30)
def test_sentinels__exist(create_sentinel, start_sentinel, start_server, loop):
    m1 = start_server('master-two-sentinels')
    s1 = start_sentinel('peer-sentinel-1', m1, quorum=2, noslaves=True)
    s2 = start_sentinel('peer-sentinel-2', m1, quorum=2, noslaves=True)

    redis_sentinel = yield from create_sentinel(
        [s1.tcp_address, s2.tcp_address])

    while True:
        info = yield from redis_sentinel.master('master-two-sentinels')
        if info['num-other-sentinels'] > 0:
            break
        yield from asyncio.sleep(.2, loop=loop)
    info = yield from redis_sentinel.sentinels('master-two-sentinels')
    assert len(info) == 1
    assert 'sentinel' in info[0]['flags']
    assert info[0]['port'] in (s1.tcp_address.port, s2.tcp_address.port)


@pytest.mark.run_loop
def test_ckquorum(redis_sentinel):
    assert (yield from redis_sentinel.check_quorum('master-no-fail'))

    # change quorum

    assert (yield from redis_sentinel.set('master-no-fail', 'quorum', 2))

    with pytest.raises(RedisError):
        yield from redis_sentinel.check_quorum('master-no-fail')

    assert (yield from redis_sentinel.set('master-no-fail', 'quorum', 1))
    assert (yield from redis_sentinel.check_quorum('master-no-fail'))


@pytest.mark.run_loop
def test_set_option(redis_sentinel):
    assert (yield from redis_sentinel.set('master-no-fail', 'quorum', 10))
    master = yield from redis_sentinel.master('master-no-fail')
    assert master['quorum'] == 10

    assert (yield from redis_sentinel.set('master-no-fail', 'quorum', 1))
    master = yield from redis_sentinel.master('master-no-fail')
    assert master['quorum'] == 1

    with pytest.raises(ReplyError):
        yield from redis_sentinel.set('masterA', 'foo', 'bar')


@pytest.mark.run_loop
def test_sentinel_role(sentinel, create_redis, loop):
    redis = yield from create_redis(sentinel.tcp_address, loop=loop)
    info = yield from redis.role()
    assert info.role == 'sentinel'
    assert isinstance(info.masters, list)
    assert 'master-no-fail' in info.masters


@pytest.mark.run_loop(timeout=30)
def test_remove(redis_sentinel, start_server, loop):
    m1 = start_server('master-to-remove')
    ok = yield from redis_sentinel.monitor(
        m1.name, '127.0.0.1', m1.tcp_address.port, 1)
    assert ok

    ok = yield from redis_sentinel.remove(m1.name)
    assert ok

    with pytest.raises(ReplyError):
        yield from redis_sentinel.remove('unknown-master')


@pytest.mark.run_loop(timeout=30)
def test_monitor(redis_sentinel, start_server, loop, unused_port):
    m1 = start_server('master-to-monitor')
    ok = yield from redis_sentinel.monitor(
        m1.name, '127.0.0.1', m1.tcp_address.port, 1)
    assert ok

    _, port = yield from redis_sentinel.master_address('master-to-monitor')
    assert port == m1.tcp_address.port
