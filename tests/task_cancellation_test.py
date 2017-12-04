import pytest

import asyncio


@pytest.mark.run_loop
async def test_future_cancellation(create_connection, loop, server):
    conn = await create_connection(
        server.tcp_address, loop=loop)

    ts = loop.time()
    fut = conn.execute('BLPOP', 'some-list', 5)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(fut, 1, loop=loop)
    assert fut.cancelled()

    # NOTE: Connection becomes available only after timeout expires
    await conn.execute('TIME')
    dt = int(loop.time() - ts)
    assert dt in {4, 5, 6}
    # self.assertAlmostEqual(dt, 5.0, delta=1)  # this fails too often
