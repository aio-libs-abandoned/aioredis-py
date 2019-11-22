
async def test_ssl_connection(create_connection, server, ssl_proxy):
    ssl_port, ssl_ctx = ssl_proxy(server.tcp_address.port)

    conn = await create_connection(
        ('localhost', ssl_port), ssl=ssl_ctx)
    res = await conn.execute('ping')
    assert res == b'PONG'


async def test_ssl_redis(create_redis, server, ssl_proxy):
    ssl_port, ssl_ctx = ssl_proxy(server.tcp_address.port)

    redis = await create_redis(
        ('localhost', ssl_port), ssl=ssl_ctx)
    res = await redis.ping()
    assert res == b'PONG'


async def test_ssl_pool(create_pool, server, ssl_proxy):
    ssl_port, ssl_ctx = ssl_proxy(server.tcp_address.port)

    pool = await create_pool(
        ('localhost', ssl_port), ssl=ssl_ctx)
    with (await pool) as conn:
        res = await conn.execute('PING')
        assert res == b'PONG'
