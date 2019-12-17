import asyncio
import sys
import threading
import time
import uuid

from asyncio.locks import Lock as _Lock

from .errors import LockError, LockNotOwnedError

# Fixes an issue with all Python versions that leaves pending waiters
# without being awakened when the first waiter is canceled.
# Code adapted from the PR https://github.com/python/cpython/pull/1031
# Waiting once it is merged to make a proper condition to relay on
# the stdlib implementation or this one patched


class Lock(_Lock):

    if sys.version_info < (3, 7, 0):
        async def acquire(self):
            """Acquire a lock.
            This method blocks until the lock is unlocked, then sets it to
            locked and returns True.
            """
            if not self._locked and all(w.cancelled() for w in self._waiters):
                self._locked = True
                return True

            fut = self._loop.create_future()

            self._waiters.append(fut)
            try:
                await fut
                self._locked = True
                return True
            except asyncio.CancelledError:
                if not self._locked:  # pragma: no cover
                    self._wake_up_first()
                raise
            finally:
                self._waiters.remove(fut)

        def _wake_up_first(self):
            """Wake up the first waiter who isn't cancelled."""
            for fut in self._waiters:
                if not fut.done():
                    fut.set_result(True)
                    break


class dummy:
    """Instances of this class can be used as an attribute container."""


class RedisLock:
    """
    A shared, distributed Lock. Using Redis for locking allows the Lock
    to be shared across processes and/or machines.

    It's left to the user to resolve deadlock issues and make sure
    multiple clients play nicely together.
    """

    # KEYS[1] - lock name
    # ARGS[1] - token
    # return 1 if the lock was released, otherwise 0
    LUA_RELEASE_SCRIPT = '''
        local token = redis.call('get', KEYS[1])
        if not token or token ~= ARGV[1] then
            return 0
        end
        redis.call('del', KEYS[1])
        return 1
    '''

    # KEYS[1] - lock name
    # ARGS[1] - token
    # ARGS[2] - additional milliseconds
    # return 1 if the locks time was extended, otherwise 0
    LUA_EXTEND_SCRIPT = '''
        local token = redis.call('get', KEYS[1])
        if not token or token ~= ARGV[1] then
            return 0
        end
        local expiration = redis.call('pttl', KEYS[1])
        if not expiration then
            expiration = 0
        end
        if expiration < 0 then
            return 0
        end
        redis.call('pexpire', KEYS[1], expiration + ARGV[2])
        return 1
    '''

    # KEYS[1] - lock name
    # ARGS[1] - token
    # ARGS[2] - milliseconds
    # return 1 if the locks time was reacquired, otherwise 0
    LUA_REACQUIRE_SCRIPT = '''
        local token = redis.call('get', KEYS[1])
        if not token or token ~= ARGV[1] then
            return 0
        end
        redis.call('pexpire', KEYS[1], ARGV[2])
        return 1
    '''

    def __init__(self, redis, name, *, timeout=None, sleep=0.1,
                 blocking=True, blocking_timeout=None, thread_local=True):
        """
        Create a new Lock instance named ``name`` using the Redis client
        supplied by ``redis``.

        ``timeout`` indicates a maximum life for the lock.
        By default, it will remain locked until release() is called.
        ``timeout`` can be specified as a float or integer, both representing
        the number of seconds to wait.

        ``sleep`` indicates the amount of time to sleep per loop iteration
        when the lock is in blocking mode and another client is currently
        holding the lock.

        ``blocking`` indicates whether calling ``acquire`` should block until
        the lock has been acquired or to fail immediately, causing ``acquire``
        to return False and the lock not being acquired. Defaults to True.
        Note this value can be overridden by passing a ``blocking``
        argument to ``acquire``.

        ``blocking_timeout`` indicates the maximum amount of time in seconds to
        spend trying to acquire the lock. A value of ``None`` indicates
        continue trying forever. ``blocking_timeout`` can be specified as a
        float or integer, both representing the number of seconds to wait.

        ``thread_local`` indicates whether the lock token is placed in
        thread-local storage. By default, the token is placed in thread local
        storage so that a thread only sees its token, not a token set by
        another thread. Consider the following timeline:

            time: 0, thread-1 acquires `my-lock`, with a timeout of 5 seconds.
                     thread-1 sets the token to "abc"
            time: 1, thread-2 blocks trying to acquire `my-lock` using the
                     Lock instance.
            time: 5, thread-1 has not yet completed. redis expires the lock
                     key.
            time: 5, thread-2 acquired `my-lock` now that it's available.
                     thread-2 sets the token to "xyz"
            time: 6, thread-1 finishes its work and calls release(). if the
                     token is *not* stored in thread local storage, then
                     thread-1 would see the token value as "xyz" and would be
                     able to successfully release the thread-2's lock.

        In some use cases it's necessary to disable thread local storage. For
        example, if you have code where one thread acquires a lock and passes
        that lock instance to a worker thread to release later. If thread
        local storage isn't disabled in this case, the worker thread won't see
        the token set by the thread that acquired the lock. Our assumption
        is that these cases aren't common and as such default to using
        thread local storage.
        """
        self.redis = redis
        self.name = name
        self.timeout = timeout
        self.sleep = sleep
        self.blocking = blocking
        self.blocking_timeout = blocking_timeout
        self.thread_local = bool(thread_local)
        self.local = threading.local() if self.thread_local else dummy()
        self.local.token = None
        if self.timeout and self.sleep > self.timeout:
            raise LockError("'sleep' must be less than 'timeout'")

    async def __aenter__(self):
        # force blocking, as otherwise the user would have to check whether
        # the lock was actually acquired or not.
        if await self.acquire(blocking=True):
            return self

        raise LockError('Unable to acquire lock within the time specified')

    async def __aexit__(self, exc_type, exc, tb):
        await self.release()

    async def acquire(self, *, blocking=None, blocking_timeout=None,
                      token=None):
        """
        Use Redis to hold a shared, distributed lock named ``name``.
        Returns True once the lock is acquired.

        If ``blocking`` is False, always return immediately. If the lock
        was acquired, return True, otherwise return False.

        ``blocking_timeout`` specifies the maximum number of seconds to
        wait trying to acquire the lock.

        ``token`` specifies the token value to be used. If provided, token
        must be a string that be encoded using utf-8. If a token isn't
        specified, a UUID will be generated.
        """
        sleep = self.sleep
        if token is None:
            token = uuid.uuid4().hex

        if blocking is None:
            blocking = self.blocking

        if blocking_timeout is None:
            blocking_timeout = self.blocking_timeout

        stop_trying_at = None
        if blocking_timeout is not None:
            stop_trying_at = time.time() + blocking_timeout

        while True:
            if await self.do_acquire(token):
                self.local.token = token
                return True

            if not blocking:
                return False

            if stop_trying_at is not None and time.time() > stop_trying_at:
                return False

            await asyncio.sleep(sleep)

    async def do_acquire(self, token):
        if self.timeout:
            # convert to milliseconds
            timeout = int(self.timeout * 1000)
        else:
            timeout = None

        if await self.redis.set(self.name, token,
                                exist=self.redis.SET_IF_NOT_EXIST,
                                pexpire=timeout):
            return True

        return False

    async def locked(self):
        """
        Returns True if this key is locked by any process, otherwise False.
        """
        return await self.redis.get(self.name, encoding='utf-8') is not None

    async def owned(self):
        """
        Returns True if this key is locked by this lock, otherwise False.
        """
        stored_token = await self.redis.get(self.name, encoding='utf-8')
        return self.local.token is not None and \
            stored_token == self.local.token

    async def release(self):
        """Releases the already acquired lock."""
        expected_token = self.local.token
        if expected_token is None:
            raise LockError('Cannot release an unlocked lock')

        self.local.token = None
        await self.do_release(expected_token)

    async def do_release(self, expected_token):
        if not bool(await self.redis.eval(self.LUA_RELEASE_SCRIPT,
                                          keys=[self.name],
                                          args=[expected_token])):
            raise LockNotOwnedError("Cannot release a lock"
                                    " that's no longer owned")

    async def extend(self, additional_time):
        """Adds more time to an already acquired lock.

        ``additional_time`` can be specified as an integer or a float, both
        representing the number of seconds to add.
        """
        if self.local.token is None:
            raise LockError('Cannot extend an unlocked lock')

        if self.timeout is None:
            raise LockError('Cannot extend a lock with no timeout')

        return await self.do_extend(additional_time)

    async def do_extend(self, additional_time):
        additional_time = int(additional_time * 1000)
        if not bool(await self.redis.eval(self.LUA_EXTEND_SCRIPT,
                                          keys=[self.name],
                                          args=[self.local.token,
                                                additional_time])):
            raise LockNotOwnedError("Cannot extend a lock"
                                    " that's no longer owned")

        return True

    async def reacquire(self):
        """Resets a TTL of an already acquired lock back to a timeout value."""
        if self.local.token is None:
            raise LockError('Cannot reacquire an unlocked lock')

        if self.timeout is None:
            raise LockError('Cannot reacquire a lock with no timeout')

        return await self.do_reacquire()

    async def do_reacquire(self):
        timeout = int(self.timeout * 1000)
        if not bool(await self.redis.eval(self.LUA_REACQUIRE_SCRIPT,
                                          keys=[self.name],
                                          args=[self.local.token, timeout])):
            raise LockNotOwnedError("Cannot reacquire a lock"
                                    " that's no longer owned")

        return True
