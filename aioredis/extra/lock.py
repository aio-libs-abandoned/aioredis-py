# -*- coding: utf-8 -*-
import time
import random
import asyncio

from aioredis.errors import ReplyError


class LockError(Exception):
    pass


class _LockContextManager:

    __slots__ = ('_lock', )

    def __init__(self, lock):
        self._lock = lock

    def __enter__(self):
        return self._lock

    def __exit__(self, exc_type, exc_value, tb):
        asyncio.Task(self._lock.release(), loop=self._lock._loop)


class Lock:
    """
        Lock using set (with NX and EX params) and Lua script.
        http://redis.io/commands/set (Algorithm documentation)
    """
    release_script = """
    if redis.call("get",KEYS[1]) == ARGV[1]
    then
        return redis.call("del",KEYS[1])
    else
        return 0
    end
    """
    release_digest = "RELEASE_DIGEST"

    def __init__(self, redis, key, timeout=None, *,
                 sleep=0.1, sleep_factor=None, blocking_timeout=None):
        self.redis = redis
        self.key = key
        self.timeout = timeout
        self.sleep = sleep
        self.sleep_factor = sleep_factor
        self.blocking_timeout = blocking_timeout
        self._token = bytes(random.randrange(256) for i in range(20))
        if self.timeout and self.sleep > self.timeout:
            raise LockError("'sleep' must be less than 'timeout'")
        if sleep_factor is not None:
            assert sleep_factor > 1, "'sleep_factor' must be > 1"
        self._loop = self.redis.connection._loop

    @asyncio.coroutine
    def acquire(self):
        sleep = self.sleep
        sleep_factor = self.sleep_factor
        blocking_timeout = self.blocking_timeout
        loop = self._loop

        stop_trying_at = None
        if blocking_timeout is not None:
            stop_trying_at = time.time() + blocking_timeout

        while True:
            if blocking_timeout is not None:
                acquired = yield from asyncio.wait_for(self._acquire(),
                                                       blocking_timeout,
                                                       loop=loop)
            else:
                acquired = yield from self._acquire()
            if acquired:
                return True
            elif blocking_timeout is None or time.time() > stop_trying_at:
                return False
            yield from asyncio.sleep(sleep, loop=loop)
            if sleep_factor is not None:
                sleep *= sleep_factor

    def _acquire(self):
        return self.redis.set(self.key, self._token,
                              exist=self.redis.SET_IF_NOT_EXIST,
                              **{'expire': self.timeout}
                              if self.timeout else {})

    def release(self):
        if self._token is None:
            raise LockError("Cannot release an unlocked lock")
        return self._release()

    @asyncio.coroutine
    def _release(self):
        """ Delete locked key if token is identical """
        res = yield from self.execute_script(self.release_script,
                                             self.release_digest,
                                             keys=[self.key],
                                             args=[self._token])
        if res:
            return True
        else:
            raise LockError("Cannot release a lock that's no longer owned")

    @asyncio.coroutine
    def execute_script(self, script, digest, *, keys, args):
        redis = self.redis
        try:
            res = yield from redis.evalsha(digest, keys=keys, args=args)
        except ReplyError as e:
            if "NOSCRIPT" in str(e):
                digest = yield from redis.script_load(script)
                res = yield from redis.evalsha(digest, keys=keys, args=args)
                self.__class__.release_digest = digest
            else:
                raise
        return res

    def __enter__(self):
        raise RuntimeError("'yield from' should be used as a "
                           "context manager expression")

    def __exit__(self, *args):
        pass

    def __iter__(self):
        acquired = yield from self.acquire()
        if not acquired:
            raise LockError("Lock not acquired")
        return _LockContextManager(self)
