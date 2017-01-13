import pytest


@pytest.mark.run_loop
async def test_await(create_pool, server, loop):
    pool = await create_pool(
        server.tcp_address,
        minsize=10, loop=loop)

    with await pool as conn:
        msg = await conn.execute('echo', 'hello')
        assert msg == b'hello'


@pytest.mark.run_loop
async def test_async_with(create_pool, server, loop):
    pool = await create_pool(
        server.tcp_address,
        minsize=10, loop=loop)

    async with pool.get() as conn:
        msg = await conn.execute('echo', 'hello')
        assert msg == b'hello'
