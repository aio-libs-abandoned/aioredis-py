import asyncio
import secrets
import time

from asyncio.locks import Lock as _Lock
from asyncio import coroutine

# Fixes an issue with all Python versions that leaves pending waiters
# without being awakened when the first waiter is canceled.
# Code adapted from the PR https://github.com/python/cpython/pull/1031
# Waiting once it is merged to make a proper condition to relay on
# the stdlib implementation or this one patched


class Lock(_Lock):

    @coroutine
    def acquire(self):
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
            yield from fut
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


class UnableToLockError(Exception):
    pass


class RedisLock:
    """
    This is the simple implementation from the redis documentation for
    distributed locking, influenced by redis-py's implementation.
    """

    # Script to acquire the lock and make sure it will expire
    # eventually
    # param: keys[1] - key to lock on (shared)
    # param: argv[1] - this lock's token (unique)
    # param: argv[2] - expiration in milliseconds
    ACQUIRE_SCRIPT = """
        if redis.call('setnx', KEYS[1], ARGV[1]) == 1 then
            redis.call('pexpire', KEYS[1], ARGV[2])
            return 1
        else
            return 0
        end
    """

    # Script to release the lock, this will only delete
    # the lock token if it's the one from this lock instance
    # param: keys[1] - key to lock on (shared)
    # param: args[1] - this lock's token (unique)
    # returns: 1 if released, otherwise 0
    RELEASE_SCRIPT = """
        if redis.call('get', KEYS[1]) == ARGV[1] then
            return redis.call('del', KEYS[1])
        else
            return 0
        end
    """

    # Extend the lock, this will only extend if the
    # current lock holder is this lock instance.
    # param: keys[1] - key to lock on (shared)
    # param: args[1] - this lock's token (unique)
    # param: args[2] - additional millis to keep the lock
    # returns: 1 if extended, otherwise 0
    EXTEND_SCRIPT = """
        if redis.call('get', KEYS[1]) ~= ARGV[1] then
            return 0
        end

        local expiration = redis.call('pttl', KEYS[1])
        if expiration < 0 then
            return 0
        end
        redis.call('pexpire', KEYS[1], expiration + ARGV[2])
        return 1
    """

    def __init__(self, pool_or_conn, key, timeout=30, wait_timeout=30):
        self._pool_or_conn = pool_or_conn
        self._key = key
        self._timeout = timeout
        self._wait_timeout = wait_timeout
        self._token = secrets.token_hex(6)

        self._acquire_script = None
        self._extend_script = None
        self._release_script = None

    async def register_scripts(self):
        """
        Register the required scripts with redis
        """
        if self._acquire_script is None:
            self._acquire_script = await self._pool_or_conn.script_load(self.ACQUIRE_SCRIPT)
        if self._release_script is None:
            self._release_script = await self._pool_or_conn.script_load(self.RELEASE_SCRIPT)
        if self._extend_script is None:
            self._extend_script = await self._pool_or_conn.script_load(self.EXTEND_SCRIPT)

    async def __aenter__(self):
        await self.register_scripts()

        if await self.acquire(self._key, self._timeout, self._wait_timeout):
            return self

        raise UnableToLockError("Unable to acquire lock within timeout")

    async def __aexit__(self, *args, **kwargs):
        await self.release()

    async def acquire(self, key, timeout=30, wait_timeout=30):
        """
        Attempt to acquire the lock
        :param key: Lock key
        :param timeout: Number of seconds until the lock should timeout. It can
                        be extended via extend
        :param wait_timeout: How long to wait before aborting the lock request
        """
        start = int(time.time())
        while True:
            if await self._script_exec(
                self._acquire_script,
                keys=[self._key],
                args=[self._token, timeout * 1_000]
            ):
                return True

            if int(time.time()) - start > wait_timeout:
                return False
            await asyncio.sleep(0.1)

    async def extend(self, added_time):
        """
        Extend the lock by the amount of time, this will only extend
        if this instance owns the lock.

        :param added_time: Number of seconds to add to the lock expiration
        :returns: 1 if the extend was successful, 0 otherwise
        """
        return await self._script_exec(
            self._extend_script,
            keys=[self._key],
            args=[self._token, added_time * 1_000],
        )

    async def release(self):
        """
        Release the lock, this will only release if this instance of the lock
        is the one holding the lock.

        :returns: 1 if the release was successful, 0 otherwise.
        """
        return await self._script_exec(
            self._release_script,
            keys=[self._key],
            args=[self._token]
        )

    async def _script_exec(self, sha, keys, args):
        """
        Execute the script with the provided keys and args.

        :param sha: Script sha, returned after the script was loaded
        :param keys: Keys to the script
        :param args: Args to the script
        """
        return await self._pool_or_conn.evalsha(sha, keys=keys, args=args)
