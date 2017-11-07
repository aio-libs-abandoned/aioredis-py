import pytest
import asyncio

from aioredis import ConnectionClosedError, ReplyError
from aioredis.pool import ConnectionsPool
from aioredis import Redis


@pytest.mark.run_loop
def test_repr(create_redis, loop, server):
    redis = yield from create_redis(
        server.tcp_address, db=1, loop=loop)
    assert repr(redis) in {
        '<Redis <RedisConnection [db:1]>>',
        '<Redis <ConnectionsPool [db:1, size:[1:10], free:1]>>',
        }

    redis = yield from create_redis(
        server.tcp_address, db=0, loop=loop)
    assert repr(redis) in {
        '<Redis <RedisConnection [db:0]>>',
        '<Redis <ConnectionsPool [db:0, size:[1:10], free:1]>>',
        }


@pytest.mark.run_loop
def test_auth(redis):
    expected_message = "ERR Client sent AUTH, but no password is set"
    with pytest.raises(ReplyError, match=expected_message):
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


@pytest.mark.run_loop
def test_quit(redis, loop):
    expected = (ConnectionClosedError, ConnectionError)
    try:
        assert b'OK' == (yield from redis.quit())
    except expected:
        pass

    if not isinstance(redis.connection, ConnectionsPool):
        # reader task may not yet been cancelled and _do_close not called
        #   so the ConnectionClosedError may be raised (or ConnectionError)
        with pytest.raises(expected):
            try:
                yield from redis.ping()
            except asyncio.CancelledError:
                assert False, "Cancelled error must not be raised"

        # wait one loop iteration until it get surely closed
        yield from asyncio.sleep(0, loop=loop)
        assert redis.connection.closed

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


@pytest.mark.run_loop
def test_yield_from_backwards_compatability(create_redis, server, loop):
    redis = yield from create_redis(server.tcp_address, loop=loop)

    assert isinstance(redis, Redis)
    # TODO: there should not be warning
    # with pytest.warns(UserWarning):
    with (yield from redis) as client:
        assert isinstance(client, Redis)
        assert client is not redis
        assert (yield from client.ping())
