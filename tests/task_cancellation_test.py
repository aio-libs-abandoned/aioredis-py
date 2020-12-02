import asyncio

import pytest


@pytest.mark.asyncio
async def test_future_cancellation(create_connection, event_loop, server):
    conn = await create_connection(server.tcp_address)

    ts = event_loop.time()
    fut = conn.execute("BLPOP", "some-list", 5)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(fut, 1)
    assert fut.cancelled()

    # NOTE: Connection becomes available only after timeout expires
    await conn.execute("TIME")
    dt = int(event_loop.time() - ts)
    assert dt in {4, 5, 6}
    # self.assertAlmostEqual(dt, 5.0, delta=1)  # this fails too often
