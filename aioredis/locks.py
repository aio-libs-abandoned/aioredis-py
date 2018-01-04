from asyncio.locks import Lock as _Lock
from asyncio import coroutine
from asyncio import futures

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
        except futures.CancelledError:
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
