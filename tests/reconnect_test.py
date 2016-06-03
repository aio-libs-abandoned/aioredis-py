import pytest

from aioredis import create_reconnecting_redis


@pytest.mark.run_loop
def test_recon(server, loop):
    redis = yield from create_reconnecting_redis(
        server.tcp_address, db=1, loop=loop)
    assert repr(redis) == '<Redis <AutoConnector None>>'
    resp = yield from redis.echo('ECHO')
    assert resp == b'ECHO'
    assert repr(redis) == '<Redis <AutoConnector <RedisConnection [db:1]>>>'
    conn_id = id(redis._conn._conn)

    redis._conn._conn._do_close(ValueError("Emulate connection close"))

    resp = yield from redis.echo('ECHO')
    assert resp == b'ECHO'
    assert conn_id != id(redis._conn._conn)
    # FIXME: bad interface
    conn = yield from redis.connection.get_atomic_connection()
    conn.close()
    yield from conn.wait_closed()


@pytest.mark.run_loop
def test_multi_exec(server, loop):
    redis = yield from create_reconnecting_redis(
        server.tcp_address, db=1, loop=loop)
    assert repr(redis) == '<Redis <AutoConnector None>>'

    m = redis.multi_exec()
    m.echo('ECHO')
    res = yield from m.execute()
    assert res == [b'ECHO']
    # FIXME: bad interface
    conn = yield from redis.connection.get_atomic_connection()
    conn.close()
    yield from conn.wait_closed()
