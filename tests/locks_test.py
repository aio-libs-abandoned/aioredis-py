import asyncio

from aioredis.locks import Lock


async def test_finished_waiter_cancelled():
    lock = Lock()

    ta = asyncio.ensure_future(lock.acquire())
    await asyncio.sleep(0)
    assert lock.locked()

    tb = asyncio.ensure_future(lock.acquire())
    await asyncio.sleep(0)
    assert len(lock._waiters) == 1

    # Create a second waiter, wake up the first, and cancel it.
    # Without the fix, the second was not woken up and the lock
    # will never be locked
    asyncio.ensure_future(lock.acquire())
    await asyncio.sleep(0)
    lock.release()
    tb.cancel()

    await asyncio.sleep(0)
    assert ta.done()
    assert tb.cancelled()

    await asyncio.sleep(0)
    assert lock.locked()
