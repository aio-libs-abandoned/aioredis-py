import pytest


@pytest.mark.run_loop
def test_ssl_connection(create_connection, loop, server, ssl_proxy):
    ssl_port, ssl_ctx = ssl_proxy(server.tcp_address.port)

    conn = yield from create_connection(
        ('localhost', ssl_port), ssl=ssl_ctx, loop=loop)
    res = yield from conn.execute('ping')
    assert res == b'PONG'


@pytest.mark.run_loop
def test_ssl_redis(create_redis, loop, server, ssl_proxy):
    ssl_port, ssl_ctx = ssl_proxy(server.tcp_address.port)

    redis = yield from create_redis(
        ('localhost', ssl_port), ssl=ssl_ctx, loop=loop)
    res = yield from redis.ping()
    assert res == b'PONG'


@pytest.mark.run_loop
def test_ssl_pool(create_pool, server, loop, ssl_proxy):
    ssl_port, ssl_ctx = ssl_proxy(server.tcp_address.port)

    pool = yield from create_pool(
        ('localhost', ssl_port), ssl=ssl_ctx, loop=loop)
    with (yield from pool) as redis:
        res = yield from redis.ping()
        assert res == b'PONG'
