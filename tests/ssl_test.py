import pytest


@pytest.mark.run_loop
async def test_ssl_connection(create_connection, loop, server, ssl_proxy):
    ssl_port, ssl_ctx = ssl_proxy(server.tcp_address.port)

    conn = await create_connection(
        ('localhost', ssl_port), ssl=ssl_ctx, loop=loop)
    res = await conn.execute('ping')
    assert res == b'PONG'


@pytest.mark.run_loop
async def test_ssl_redis(create_redis, loop, server, ssl_proxy):
    ssl_port, ssl_ctx = ssl_proxy(server.tcp_address.port)

    redis = await create_redis(
        ('localhost', ssl_port), ssl=ssl_ctx, loop=loop)
    res = await redis.ping()
    assert res == b'PONG'


@pytest.mark.run_loop
async def test_ssl_pool(create_pool, server, loop, ssl_proxy):
    ssl_port, ssl_ctx = ssl_proxy(server.tcp_address.port)

    pool = await create_pool(
        ('localhost', ssl_port), ssl=ssl_ctx, loop=loop)
    with (await pool) as conn:
        res = await conn.execute('PING')
        assert res == b'PONG'
