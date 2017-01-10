import pytest
import sys

from aioredis import ConnectionClosedError, ReplyError


@pytest.mark.run_loop
def test_repr(create_redis, loop, server):
    redis = yield from create_redis(
        server.tcp_address, db=1, loop=loop)
    assert repr(redis) == '<Redis <RedisConnection [db:1]>>'

    redis = yield from create_redis(
        server.tcp_address, db=0, loop=loop)
    assert repr(redis) == '<Redis <RedisConnection [db:0]>>'


@pytest.mark.run_loop
def test_auth(redis):
    expected_message = "ERR Client sent AUTH, but no password is set"
    with pytest.raises_regex(ReplyError, expected_message):
        yield from redis.auth('')


@pytest.mark.run_loop
def test_echo(redis):
    resp = yield from redis.echo('ECHO')
    assert resp == b'ECHO'

    with pytest.raises(TypeError):
        yield from redis.echo(None)


@pytest.mark.run_loop
def test_ping(redis):
    resp = yield from redis.ping()
    assert resp == b'PONG'


@pytest.mark.xfail(sys.platform == 'win32',
                   reason="Probably race conditions...")
@pytest.mark.run_loop
def test_quit(redis):
    resp = yield from redis.quit()
    assert resp == b'OK'

    with pytest.raises(ConnectionClosedError):
        yield from redis.ping()


@pytest.mark.run_loop
def test_select(redis):
    assert redis.db == 0

    resp = yield from redis.select(1)
    assert resp is True
    assert redis.db == 1
    assert redis.connection.db == 1


@pytest.mark.run_loop
def test_encoding(create_redis, loop, server):
    redis = yield from create_redis(
        server.tcp_address,
        db=1, encoding='utf-8',
        loop=loop)
    assert redis.encoding == 'utf-8'
