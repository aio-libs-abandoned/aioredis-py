import asyncio
import pytest

from aioredis.locks import Lock


@pytest.mark.run_loop
async def test_finished_waiter_cancelled(loop):
    lock = Lock(loop=loop)

    ta = asyncio.ensure_future(lock.acquire(), loop=loop)
    await asyncio.sleep(0, loop=loop)
    assert lock.locked()

    tb = asyncio.ensure_future(lock.acquire(), loop=loop)
    await asyncio.sleep(0, loop=loop)
    assert len(lock._waiters) == 1

    # Create a second waiter, wake up the first, and cancel it.
    # Without the fix, the second was not woken up and the lock
    # will never be locked
    asyncio.ensure_future(lock.acquire(), loop=loop)
    await asyncio.sleep(0, loop=loop)
    lock.release()
    tb.cancel()

    await asyncio.sleep(0, loop=loop)
    assert ta.done()
    assert tb.cancelled()

    await asyncio.sleep(0, loop=loop)
    assert lock.locked()
