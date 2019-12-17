import asyncio
import pytest
import time

from aioredis.errors import LockError, LockNotOwnedError
from aioredis.locks import Lock, RedisLock


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


async def test_lock(redis):
    lock = RedisLock(redis, 'foo')
    assert await lock.acquire(blocking=False)
    assert await redis.get('foo', encoding='utf-8') == lock.local.token
    assert await redis.ttl('foo') == -1
    await lock.release()
    assert await redis.get('foo', encoding='utf-8') is None


async def test_lock_token(redis):
    lock = RedisLock(redis, 'foo')
    assert await lock.acquire(blocking=False, token='test')
    assert await redis.get('foo', encoding='utf-8') == 'test'
    assert lock.local.token == 'test'
    assert await redis.ttl('foo') == -1
    await lock.release()
    assert await redis.get('foo', encoding='utf-8') is None
    assert lock.local.token is None


async def test_locked(redis):
    lock = RedisLock(redis, 'foo')
    assert await lock.locked() is False
    await lock.acquire(blocking=False)
    assert await lock.locked() is True
    await lock.release()
    assert await lock.locked() is False


async def test_owned(redis):
    lock = RedisLock(redis, 'foo')
    assert await lock.owned() is False
    await lock.acquire(blocking=False)
    assert await lock.owned() is True
    await lock.release()
    assert await lock.owned() is False

    lock2 = RedisLock(redis, 'foo')
    assert await lock.owned() is False
    assert await lock2.owned() is False
    await lock2.acquire(blocking=False)
    assert await lock.owned() is False
    assert await lock2.owned() is True
    await lock2.release()
    assert await lock.owned() is False
    assert await lock2.owned() is False


async def test_competing_locks(redis):
    lock1 = RedisLock(redis, 'foo')
    lock2 = RedisLock(redis, 'foo')
    assert await lock1.acquire(blocking=False)
    assert not await lock2.acquire(blocking=False)
    await lock1.release()
    assert await lock2.acquire(blocking=False)
    assert not await lock1.acquire(blocking=False)
    await lock2.release()


async def test_timeout(redis):
    lock = RedisLock(redis, 'foo', timeout=10)
    assert await lock.acquire(blocking=False)
    assert 8 < await redis.ttl('foo') <= 10
    await lock.release()


async def test_float_timeout(redis):
    lock = RedisLock(redis, 'foo', timeout=9.5)
    assert await lock.acquire(blocking=False)
    assert 8 < await redis.pttl('foo') <= 9500
    await lock.release()


async def test_blocking_timeout(redis):
    lock1 = RedisLock(redis, 'foo')
    assert await lock1.acquire(blocking=False)
    lock2 = RedisLock(redis, 'foo', blocking_timeout=0.2)
    start = time.time()
    assert not await lock2.acquire()
    assert (time.time() - start) > 0.2
    await lock1.release()


async def test_context_manager(redis):
    # blocking_timeout prevents a deadlock if the lock can't be acquired
    # for some reason
    async with RedisLock(redis, 'foo', blocking_timeout=0.2) as lock:
        assert await redis.get('foo', encoding='utf-8') == lock.local.token

    assert await redis.get('foo', encoding='utf-8') is None


async def test_context_manager_raises_when_lock_not_acquired(redis):
    await redis.set('foo', 'bar')
    with pytest.raises(LockError):
        async with RedisLock(redis, 'foo', blocking_timeout=0.1):
            pass  # pragma: no cover


async def test_high_sleep_raises_error(redis):
    # If sleep is higher than timeout, it should raise an error
    with pytest.raises(LockError):
        RedisLock(redis, 'foo', timeout=1, sleep=2)


async def test_releasing_unlocked_lock_raises_error(redis):
    lock = RedisLock(redis, 'foo')
    with pytest.raises(LockError):
        await lock.release()


async def test_releasing_lock_no_longer_owned_raises_error(redis):
    lock = RedisLock(redis, 'foo')
    await lock.acquire(blocking=False)

    # manually change the token
    await redis.set('foo', 'a')
    with pytest.raises(LockNotOwnedError):
        await lock.release()

    # even though we errored, the token is still cleared
    assert lock.local.token is None


async def test_extend_lock(redis):
    lock = RedisLock(redis, 'foo', timeout=10)
    assert await lock.acquire(blocking=False)
    assert 8000 < await redis.pttl('foo') <= 10000
    assert await lock.extend(10)
    assert 16000 < await redis.pttl('foo') <= 20000
    await lock.release()


async def test_extend_lock_float(redis):
    lock = RedisLock(redis, 'foo', timeout=10.0)
    assert await lock.acquire(blocking=False)
    assert 8000 < await redis.pttl('foo') <= 10000
    assert await lock.extend(10.0)
    assert 16000 < await redis.pttl('foo') <= 20000
    await lock.release()


async def test_extending_unlocked_lock_raises_error(redis):
    lock = RedisLock(redis, 'foo', timeout=10)
    with pytest.raises(LockError):
        await lock.extend(10)


async def test_extending_lock_with_no_timeout_raises_error(redis):
    lock = RedisLock(redis, 'foo')
    assert await lock.acquire(blocking=False)
    with pytest.raises(LockError):
        await lock.extend(10)

    await lock.release()


async def test_extending_lock_no_longer_owned_raises_error(redis):
    lock = RedisLock(redis, 'foo', timeout=10)
    assert await lock.acquire(blocking=False)
    await redis.set('foo', 'a')
    with pytest.raises(LockNotOwnedError):
        await lock.extend(10)


async def test_reacquire_lock(redis):
    lock = RedisLock(redis, 'foo', timeout=10)
    assert await lock.acquire(blocking=False)
    assert await redis.pexpire('foo', 5000)
    assert await redis.pttl('foo') <= 5000
    assert await lock.reacquire()
    assert 8000 < await redis.pttl('foo') <= 10000
    await lock.release()


async def test_reacquiring_unlocked_lock_raises_error(redis):
    lock = RedisLock(redis, 'foo', timeout=10)
    with pytest.raises(LockError):
        await lock.reacquire()


async def test_reacquiring_lock_with_no_timeout_raises_error(redis):
    lock = RedisLock(redis, 'foo')
    assert await lock.acquire(blocking=False)
    with pytest.raises(LockError):
        await lock.reacquire()

    await lock.release()


async def test_reacquiring_lock_no_longer_owned_raises_error(redis):
    lock = RedisLock(redis, 'foo', timeout=10)
    assert await lock.acquire(blocking=False)
    await redis.set('foo', 'a')
    with pytest.raises(LockNotOwnedError):
        await lock.reacquire()


async def test_lock_class_argument(redis):
    class MyLock:
        def __init__(self, *args, **kwargs):
            pass

    lock = redis.lock('foo', lock_class=MyLock)
    assert type(lock) == MyLock
