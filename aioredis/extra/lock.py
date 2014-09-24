# -*- coding: utf-8 -*-
import time
import uuid
import asyncio


class LockError(Exception):
    pass


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
    release_digest = None

    def __init__(self, redis, key, *, timeout=None, sleep=0.1,
                 blocking=True, blocking_timeout=None):
        assert all(d for d in [self.release_digest]), "Scripts not loaded"
        self.redis = redis
        self.key = key
        self.timeout = timeout
        self.sleep = sleep
        self.blocking = blocking
        self.blocking_timeout = blocking_timeout
        self._token = uuid.uuid4().hex
        if self.timeout and self.sleep > self.timeout:
            raise LockError("'sleep' must be less than 'timeout'")

    @asyncio.coroutine
    def acquire(self, blocking=None, blocking_timeout=None):
        sleep = self.sleep
        blocking = blocking or self.blocking
        blocking_timeout = blocking_timeout or self.blocking_timeout

        stop_trying_at = None
        if blocking_timeout is not None:
            stop_trying_at = time.time() + blocking_timeout
        while True:
            if (yield from self._acquire()):
                return True
            if not blocking:
                return False
            if stop_trying_at is not None and time.time() > stop_trying_at:
                return False
            yield from asyncio.sleep(sleep)

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
        res = yield from self.redis.evalsha(self.release_digest,
                                            keys=[self.key],
                                            args=[self._token])
        if res:
            return True
        else:
            raise LockError("Cannot release a lock that's no longer owned")

    @classmethod
    @asyncio.coroutine
    def register_scripts(cls, redis):
        cls.release_digest = yield from redis.script_load(cls.release_script)


class LockMixin:

    def register_lock_scripts(self):
        return Lock.register_scripts(self)

    def lock(self, key, *, timeout=None, sleep=0.1,
             blocking=True, blocking_timeout=None):
        return Lock(self, key, timeout=timeout, sleep=sleep,
                    blocking=blocking, blocking_timeout=blocking_timeout)

    @asyncio.coroutine
    def execute_in_lock(self, task, key, *, timeout=None, sleep=0.1,
                        blocking=True, blocking_timeout=None):
        lock = self.lock(key, timeout=timeout, sleep=sleep,
                         blocking=blocking, blocking_timeout=blocking_timeout)
        acquired = yield from lock.acquire()
        if not acquired:
            raise LockError("Lock not acquired")
        try:
            result = yield from task
        finally:
            yield from lock.release()
        return result
