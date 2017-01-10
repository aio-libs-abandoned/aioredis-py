import time
import pytest
import sys

from unittest import mock

from aioredis import ReplyError


@pytest.mark.run_loop
def test_client_list(redis, server):
    res = yield from redis.client_list()
    assert isinstance(res, list)
    res = [dict(i._asdict()) for i in res]
    expected = {
        'addr': mock.ANY,
        'fd': mock.ANY,
        'age': '0',
        'idle': '0',
        'flags': 'N',
        'db': '0',
        'sub': '0',
        'psub': '0',
        'multi': '-1',
        'qbuf': '0',
        'qbuf_free': mock.ANY,
        'obl': '0',
        'oll': '0',
        'omem': '0',
        'events': 'r',
        'cmd': 'client',
        'name': '',
        }
    if server.version >= (2, 8, 12):
        expected['id'] = mock.ANY
    assert res == [expected]


@pytest.mark.run_loop
@pytest.mark.skipif(sys.platform == 'win32',
                    reason="No unixsocket on Windows")
def test_client_list__unixsocket(create_redis, loop, server):
    redis = yield from create_redis(server.unixsocket, loop=loop)
    res = yield from redis.client_list()
    assert len(res) == 1
    info = dict(res[0]._asdict())
    expected = {
        'addr': '{}:0'.format(server.unixsocket),
        'fd': mock.ANY,
        'age': '0',
        'idle': '0',
        'flags': 'U',   # Conneted via unix socket
        'db': '0',
        'sub': '0',
        'psub': '0',
        'multi': '-1',
        'qbuf': '0',
        'qbuf_free': mock.ANY,
        'obl': '0',
        'oll': '0',
        'omem': '0',
        'events': 'r',
        'cmd': 'client',
        'name': '',
        }
    if server.version >= (2, 8, 12):
        expected['id'] = mock.ANY
    assert info == expected


@pytest.mark.run_loop
@pytest.redis_version(
    2, 9, 50, reason='CLIENT PAUSE is available since redis >= 2.9.50')
def test_client_pause(redis):
    res = yield from redis.client_pause(2000)
    assert res is True
    ts = time.time()
    yield from redis.ping()
    dt = int(time.time() - ts)
    assert dt == 2

    with pytest.raises(TypeError):
        yield from redis.client_pause(2.0)
    with pytest.raises(ValueError):
        yield from redis.client_pause(-1)


@pytest.mark.run_loop
def test_client_getname(redis):
    res = yield from redis.client_getname()
    assert res is None
    ok = yield from redis.client_setname('TestClient')
    assert ok is True

    res = yield from redis.client_getname()
    assert res == b'TestClient'
    res = yield from redis.client_getname(encoding='utf-8')
    assert res == 'TestClient'


@pytest.mark.run_loop
def test_config_get(redis, server):
    res = yield from redis.config_get('port')
    assert res == {'port': str(server.tcp_address.port)}

    res = yield from redis.config_get()
    assert len(res) > 0

    res = yield from redis.config_get('unknown_parameter')
    assert res == {}

    with pytest.raises(TypeError):
        yield from redis.config_get(b'port')


@pytest.mark.run_loop
def test_config_rewrite(redis):
    with pytest.raises(ReplyError):
        yield from redis.config_rewrite()


@pytest.mark.run_loop
def test_config_set(redis):
    cur_value = yield from redis.config_get('slave-read-only')
    res = yield from redis.config_set('slave-read-only', 'no')
    assert res is True
    res = yield from redis.config_set(
        'slave-read-only', cur_value['slave-read-only'])
    assert res is True

    with pytest.raises_regex(ReplyError, "Unsupported CONFIG parameter"):
        yield from redis.config_set('databases', 100)
    with pytest.raises(TypeError):
        yield from redis.config_set(100, 'databases')


@pytest.mark.run_loop
@pytest.mark.skip("Not implemented")
def test_config_resetstat():
    pass


@pytest.mark.run_loop
def test_dbsize(redis):
    res = yield from redis.dbsize()
    assert res == 0

    yield from redis.set('key', 'value')

    res = yield from redis.dbsize()
    assert res > 0

    yield from redis.flushdb()
    res = yield from redis.dbsize()
    assert res == 0
    yield from redis.set('key', 'value')
    res = yield from redis.dbsize()
    assert res == 1


@pytest.mark.run_loop
def test_info(redis):
    res = yield from redis.info()
    assert isinstance(res, dict)

    res = yield from redis.info('all')
    assert isinstance(res, dict)

    with pytest.raises(ValueError):
        yield from redis.info('')


@pytest.mark.run_loop
@pytest.redis_version(2, 8, 12, reason='ROLE is available since redis>=2.8.12')
def test_role(redis):
    res = yield from redis.role()
    assert dict(res._asdict()) == {
        'role': 'master',
        'replication_offset': mock.ANY,
        'slaves': [],
        }


@pytest.mark.run_loop
def test_time(redis):
    res = yield from redis.time()
    assert isinstance(res, float)
    assert int(res) == int(time.time())
