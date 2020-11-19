import asyncio
import sys

from asyncio.locks import Lock as _Lock

# Fixes an issue with all Python versions that leaves pending waiters
# without being awakened when the first waiter is canceled.
# Code adapted from the PR https://github.com/python/cpython/pull/1031
# Waiting once it is merged to make a proper condition to relay on
# the stdlib implementation or this one patched

# Fixes an issue with multiple lock acquire https://bugs.python.org/issue32734
# Code adapted from the PR https://github.com/python/cpython/pull/5466


class Lock(_Lock):

    if sys.version_info < (3, 6, 5):
        async def acquire(self):
            """Acquire a lock.

            This method blocks until the lock is unlocked, then sets it to
            locked and returns True.
            """
            if (not self._locked and (self._waiters is None
                                      or all(w.cancelled()
                                             for w in self._waiters))):
                self._locked = True
                return True

            fut = self._loop.create_future()
            self._waiters.append(fut)

            # Finally block should be called before the CancelledError
            # handling as we don't want CancelledError to call
            # _wake_up_first() and attempt to wake up itself.
            try:
                try:
                    await fut
                finally:
                    self._waiters.remove(fut)
            except asyncio.CancelledError:
                if not self._locked:
                    self._wake_up_first()
                raise

            self._locked = True
            return True

        def release(self):
            """Release a lock.

            When the lock is locked, reset it to unlocked, and return.
            If any other coroutines are blocked waiting for the lock to become
            unlocked, allow exactly one of them to proceed.

            When invoked on an unlocked lock, a RuntimeError is raised.

            There is no return value.
            """
            if self._locked:
                self._locked = False
                self._wake_up_first()
            else:
                raise RuntimeError('Lock is not acquired.')

        def _wake_up_first(self):
            """Wake up the first waiter if it isn't done."""
            try:
                fut = next(iter(self._waiters or []))
            except StopIteration:
                return

            # .done() necessarily means that a waiter will wake up later on and
            # either take the lock, or, if it was cancelled and lock wasn't
            # taken already, will hit this again and wake up a new waiter.
            if not fut.done():
                fut.set_result(True)
