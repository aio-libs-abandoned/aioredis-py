import time
import pytest
import sys

from unittest import mock

from aioredis import ReplyError
from _testutils import redis_version


async def test_client_list(redis, server, request):
    name = request.node.callspec.id
    assert (await redis.client_setname(name))
    res = await redis.client_list()
    assert isinstance(res, list)
    res = [dict(i._asdict()) for i in res]
    expected = {
        'addr': mock.ANY,
        'fd': mock.ANY,
        'age': mock.ANY,
        'idle': mock.ANY,
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
        'name': name,
        }
    if server.version >= (2, 8, 12):
        expected['id'] = mock.ANY
    if server.version >= (5, ):
        expected['qbuf'] = '26'
    assert expected in res


@pytest.mark.skipif(sys.platform == 'win32',
                    reason="No unixsocket on Windows")
async def test_client_list__unixsocket(create_redis, server, request):
    redis = await create_redis(server.unixsocket)
    name = request.node.callspec.id
    assert (await redis.client_setname(name))
    res = await redis.client_list()
    info = [dict(i._asdict()) for i in res]
    expected = {
        'addr': '{}:0'.format(server.unixsocket),
        'fd': mock.ANY,
        'age': mock.ANY,
        'idle': mock.ANY,
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
        'name': name,
        }
    if server.version >= (2, 8, 12):
        expected['id'] = mock.ANY
    if server.version >= (5, ):
        expected['qbuf'] = '26'
    assert expected in info


@redis_version(
    2, 9, 50, reason='CLIENT PAUSE is available since redis >= 2.9.50')
async def test_client_pause(redis):
    tr = redis.pipeline()
    tr.time()
    tr.client_pause(100)
    tr.time()
    t1, ok, t2 = await tr.execute()
    assert ok
    assert t2 - t1 >= .1

    with pytest.raises(TypeError):
        await redis.client_pause(2.0)
    with pytest.raises(ValueError):
        await redis.client_pause(-1)


async def test_client_getname(redis):
    res = await redis.client_getname()
    assert res is None
    ok = await redis.client_setname('TestClient')
    assert ok is True

    res = await redis.client_getname()
    assert res == b'TestClient'
    res = await redis.client_getname(encoding='utf-8')
    assert res == 'TestClient'


@redis_version(2, 8, 13, reason="available since Redis 2.8.13")
async def test_command(redis):
    res = await redis.command()
    assert isinstance(res, list)
    assert len(res) > 0


@redis_version(2, 8, 13, reason="available since Redis 2.8.13")
async def test_command_count(redis):
    res = await redis.command_count()
    assert res > 0


@redis_version(3, 0, 0, reason="available since Redis 3.0.0")
async def test_command_getkeys(redis):
    res = await redis.command_getkeys('get', 'key')
    assert res == ['key']
    res = await redis.command_getkeys('get', 'key', encoding=None)
    assert res == [b'key']
    res = await redis.command_getkeys('mset', 'k1', 'v1', 'k2', 'v2')
    assert res == ['k1', 'k2']
    res = await redis.command_getkeys('mset', 'k1', 'v1', 'k2')
    assert res == ['k1', 'k2']

    with pytest.raises(ReplyError):
        assert (await redis.command_getkeys('get'))
    with pytest.raises(TypeError):
        assert not (await redis.command_getkeys(None))


@redis_version(2, 8, 13, reason="available since Redis 2.8.13")
async def test_command_info(redis):
    res = await redis.command_info('get')
    assert res == [
        ['get', 2, ['readonly', 'fast'], 1, 1, 1],
    ]

    res = await redis.command_info("unknown-command")
    assert res == [None]
    res = await redis.command_info("unknown-command", "unknown-commnad")
    assert res == [None, None]


async def test_config_get(redis, server):
    res = await redis.config_get('port')
    assert res == {'port': str(server.tcp_address.port)}

    res = await redis.config_get()
    assert len(res) > 0

    res = await redis.config_get('unknown_parameter')
    assert res == {}

    with pytest.raises(TypeError):
        await redis.config_get(b'port')


async def test_config_rewrite(redis):
    with pytest.raises(ReplyError):
        await redis.config_rewrite()


async def test_config_set(redis):
    cur_value = await redis.config_get('slave-read-only')
    res = await redis.config_set('slave-read-only', 'no')
    assert res is True
    res = await redis.config_set(
        'slave-read-only', cur_value['slave-read-only'])
    assert res is True

    with pytest.raises(ReplyError, match="Unsupported CONFIG parameter"):
        await redis.config_set('databases', 100)
    with pytest.raises(TypeError):
        await redis.config_set(100, 'databases')


# @pytest.mark.skip("Not implemented")
# def test_config_resetstat():
#     pass

async def test_debug_object(redis):
    with pytest.raises(ReplyError):
        assert (await redis.debug_object('key')) is None

    ok = await redis.set('key', 'value')
    assert ok
    res = await redis.debug_object('key')
    assert res is not None


async def test_debug_sleep(redis):
    t1 = await redis.time()
    ok = await redis.debug_sleep(.2)
    assert ok
    t2 = await redis.time()
    assert t2 - t1 >= .2


async def test_dbsize(redis):
    res = await redis.dbsize()
    assert res == 0

    await redis.set('key', 'value')

    res = await redis.dbsize()
    assert res > 0

    await redis.flushdb()
    res = await redis.dbsize()
    assert res == 0
    await redis.set('key', 'value')
    res = await redis.dbsize()
    assert res == 1


async def test_info(redis):
    res = await redis.info()
    assert isinstance(res, dict)

    res = await redis.info('all')
    assert isinstance(res, dict)

    with pytest.raises(ValueError):
        await redis.info('')


async def test_lastsave(redis):
    res = await redis.lastsave()
    assert res > 0


@redis_version(2, 8, 12, reason='ROLE is available since redis>=2.8.12')
async def test_role(redis):
    res = await redis.role()
    assert dict(res._asdict()) == {
        'role': 'master',
        'replication_offset': mock.ANY,
        'slaves': [],
        }


async def test_save(redis):
    res = await redis.dbsize()
    assert res == 0
    t1 = await redis.lastsave()
    ok = await redis.save()
    assert ok
    t2 = await redis.lastsave()
    assert t2 >= t1


@pytest.mark.parametrize('encoding', [
    pytest.param(None, id='no decoding'),
    pytest.param('utf-8', id='with decoding'),
])
async def test_time(create_redis, server, encoding):
    redis = await create_redis(server.tcp_address, encoding='utf-8')
    now = time.time()
    res = await redis.time()
    assert isinstance(res, float)
    assert res == pytest.approx(now, abs=10)


async def test_slowlog_len(redis):
    res = await redis.slowlog_len()
    assert res >= 0


async def test_slowlog_get(redis):
    res = await redis.slowlog_get()
    assert isinstance(res, list)
    assert len(res) >= 0

    res = await redis.slowlog_get(2)
    assert isinstance(res, list)
    assert 0 <= len(res) <= 2

    with pytest.raises(TypeError):
        assert not (await redis.slowlog_get(1.2))
    with pytest.raises(TypeError):
        assert not (await redis.slowlog_get('1'))


async def test_slowlog_reset(redis):
    ok = await redis.slowlog_reset()
    assert ok is True
