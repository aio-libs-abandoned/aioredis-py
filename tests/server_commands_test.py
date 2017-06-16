import time
import pytest
import sys

from unittest import mock
from unittest.mock import patch
from unittest.mock import call

from aioredis import ReplyError
from aioredis.util import create_future
from aioredis.commands.server import ServerCommandsMixin


def result_future(value, loop):
    f = create_future(loop)
    f.set_result(value)
    return f


@pytest.mark.parametrize(
    "method,res,command",
    [
        (
            'bgrewriteaof',
            b'Background append only file rewriting started',
            b'BGREWRITEAOF'
        ),
        ('bgsave', b'Background saving started', b'BGSAVE')
    ]
)
@pytest.mark.run_loop
def test_bg_methods(method, res, command, redis, loop):
    with patch.object(redis, 'execute') as execute:
        execute.return_value = result_future(
            res,
            loop
        )
        f = getattr(redis, method)
        res = yield from f()
        assert res is True
        execute.assert_called_with(command)


@pytest.mark.parametrize(
    "save,command_arg",
    [
        (None, None),
        (ServerCommandsMixin.SHUTDOWN_SAVE, b"SAVE"),
        (ServerCommandsMixin.SHUTDOWN_NOSAVE, b"NOSAVE")
    ]
)
@pytest.mark.run_loop
def test_shutdown(save, command_arg, redis, loop):
    with patch.object(redis, 'execute') as execute:
        execute.return_value = result_future(
            None,
            loop
        )
        yield from redis.shutdown(save=save)

        expected = (b'SHUTDOWN',)
        if command_arg:
            expected += (command_arg,)

        execute.assert_called_with(*expected)


@pytest.mark.run_loop
def test_slaveof(redis, loop):
    with patch.object(redis, 'execute') as execute:
        execute.return_value = result_future(
            b'Ok',
            loop
        )
        assert (yield from redis.slaveof())
        assert (yield from redis.slaveof(host=None))
        assert (yield from redis.slaveof(host=b'127.0.0.1', port=6379))
        execute.assert_has_calls([
            call(b'SLAVEOF', b'NO', b'ONE'),
            call(b'SLAVEOF', b'NO', b'ONE'),
            call(b'SLAVEOF', b'127.0.0.1', 6379)
        ])


@pytest.mark.run_loop
def test_sync(redis, loop):
    with patch.object(redis, 'execute') as execute:
        execute.return_value = result_future(
            None,
            loop
        )
        yield from redis.sync()
        execute.assert_called_with(b'SYNC')


@pytest.mark.parametrize("method", ['client_kill', 'monitor'])
@pytest.mark.run_loop
def test_not_implemented_methods(method, redis, loop):
    with pytest.raises(NotImplementedError):
        f = getattr(redis, method)
        yield from f()


@pytest.mark.run_loop
def test_config_resetstat(redis):
    res = yield from redis.config_resetstat()
    assert res


@pytest.mark.run_loop
def test_client_list(redis, server, request):
    name = request.node.callspec.id
    assert (yield from redis.client_setname(name))
    res = yield from redis.client_list()
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
    assert expected in res


@pytest.mark.run_loop
@pytest.mark.skipif(sys.platform == 'win32',
                    reason="No unixsocket on Windows")
def test_client_list__unixsocket(create_redis, loop, server, request):
    redis = yield from create_redis(server.unixsocket, loop=loop)
    name = request.node.callspec.id
    assert (yield from redis.client_setname(name))
    res = yield from redis.client_list()
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
    assert expected in info


@pytest.mark.run_loop
def test_client_list_multiple_clients(create_redis, redis, server, loop):
    yield from create_redis(server.tcp_address, loop=loop)
    res = yield from redis.client_list()
    assert len(res) == 2
    assert isinstance(res[0], type(res[1]))


@pytest.mark.run_loop
@pytest.redis_version(
    2, 9, 50, reason='CLIENT PAUSE is available since redis >= 2.9.50')
def test_client_pause(redis):
    ts = time.time()
    res = yield from redis.client_pause(2000)
    assert res is True
    yield from redis.ping()
    assert int(time.time() - ts) >= 2

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


@pytest.redis_version(2, 8, 13, reason="available since Redis 2.8.13")
@pytest.mark.run_loop
def test_command(redis):
    res = yield from redis.command()
    assert isinstance(res, list)
    assert len(res) > 0


@pytest.redis_version(2, 8, 13, reason="available since Redis 2.8.13")
@pytest.mark.run_loop
def test_command_count(redis):
    res = yield from redis.command_count()
    assert res > 0


@pytest.redis_version(3, 0, 0, reason="available since Redis 3.0.0")
@pytest.mark.run_loop
def test_command_getkeys(redis):
    res = yield from redis.command_getkeys('get', 'key')
    assert res == ['key']
    res = yield from redis.command_getkeys('get', 'key', encoding=None)
    assert res == [b'key']
    res = yield from redis.command_getkeys('mset', 'k1', 'v1', 'k2', 'v2')
    assert res == ['k1', 'k2']
    res = yield from redis.command_getkeys('mset', 'k1', 'v1', 'k2')
    assert res == ['k1', 'k2']

    with pytest.raises(ReplyError):
        assert (yield from redis.command_getkeys('get'))
    with pytest.raises(TypeError):
        assert not (yield from redis.command_getkeys(None))


@pytest.redis_version(2, 8, 13, reason="available since Redis 2.8.13")
@pytest.mark.run_loop
def test_command_info(redis):
    res = yield from redis.command_info('get')
    assert res == [
        ['get', 2, ['readonly', 'fast'], 1, 1, 1],
    ]

    res = yield from redis.command_info("unknown-command")
    assert res == [None]
    res = yield from redis.command_info("unknown-command", "unknown-commnad")
    assert res == [None, None]


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

    with pytest.raises(ReplyError, match="Unsupported CONFIG parameter"):
        yield from redis.config_set('databases', 100)
    with pytest.raises(TypeError):
        yield from redis.config_set(100, 'databases')


# @pytest.mark.run_loop
# @pytest.mark.skip("Not implemented")
# def test_config_resetstat():
#     pass

@pytest.mark.run_loop
def test_debug_object(redis):
    with pytest.raises(ReplyError):
        assert (yield from redis.debug_object('key')) is None

    ok = yield from redis.set('key', 'value')
    assert ok
    res = yield from redis.debug_object('key')
    assert res is not None


@pytest.mark.run_loop
def test_debug_sleep(redis):
    t1 = yield from redis.time()
    ok = yield from redis.debug_sleep(2)
    assert ok
    t2 = yield from redis.time()
    assert t2 - t1 >= 2


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
def test_lastsave(redis):
    res = yield from redis.lastsave()
    assert res > 0


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
def test_save(redis):
    res = yield from redis.dbsize()
    assert res == 0
    t1 = yield from redis.lastsave()
    ok = yield from redis.save()
    assert ok
    t2 = yield from redis.lastsave()
    assert t2 >= t1


@pytest.mark.run_loop
def test_time(redis):
    res = yield from redis.time()
    assert isinstance(res, float)
    pytest.assert_almost_equal(int(res), int(time.time()), delta=10)


@pytest.mark.run_loop
def test_slowlog_len(redis):
    res = yield from redis.slowlog_len()
    assert res >= 0


@pytest.mark.run_loop
def test_slowlog_get(redis):
    res = yield from redis.slowlog_get()
    assert isinstance(res, list)
    assert len(res) >= 0

    res = yield from redis.slowlog_get(2)
    assert isinstance(res, list)
    assert 0 <= len(res) <= 2

    with pytest.raises(TypeError):
        assert not (yield from redis.slowlog_get(1.2))
    with pytest.raises(TypeError):
        assert not (yield from redis.slowlog_get('1'))


@pytest.mark.run_loop
def test_slowlog_reset(redis):
    ok = yield from redis.slowlog_reset()
    assert ok is True
