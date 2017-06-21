import asyncio
import pytest

from aioredis.util import async_task
from aioredis.locks import Lock


@pytest.mark.run_loop
def test_finished_waiter_cancelled(loop):
    lock = Lock(loop=loop)

    ta = async_task(lock.acquire(), loop=loop)
    yield from asyncio.sleep(0, loop=loop)
    assert lock.locked()

    tb = async_task(lock.acquire(), loop=loop)
    yield from asyncio.sleep(0, loop=loop)
    assert len(lock._waiters) == 1

    # Create a second waiter, wake up the first, and cancel it.
    # Without the fix, the second was not woken up and the lock
    # will never be locked
    async_task(lock.acquire(), loop=loop)
    yield from asyncio.sleep(0, loop=loop)
    lock.release()
    tb.cancel()

    yield from asyncio.sleep(0, loop=loop)
    assert ta.done()
    assert tb.cancelled()

    yield from asyncio.sleep(0, loop=loop)
    assert lock.locked()
