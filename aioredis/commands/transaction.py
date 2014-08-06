import asyncio

from ..errors import RedisError
from ..util import wait_ok, wait_convert


class TransactionsCommandsMixin:
    """Transaction commands mixin.

    For commands details see: http://redis.io/commands/#transactions
    """

    def discard(self):
        """Discard all commands issued after MULTI."""
        assert self._conn.in_transaction
        fut = self._conn.execute(b'DISCARD')
        return wait_ok(fut)

    def exec(self):
        """Execute all commands issued after MULTI."""
        assert self._conn.in_transaction
        fut = self._conn.execute(b'EXEC')
        return wait_convert(fut, check_errors)

    def multi(self):
        """Mark the start of a transaction block."""
        assert not self._conn.in_transaction
        fut = self._conn.execute(b'MULTI')
        return wait_ok(fut)

    def unwatch(self):
        """Forget about all watched keys."""
        fut = self._conn.execute(b'UNWATCH')
        return wait_ok(fut)

    def watch(self, key, *keys):
        """Watch the given keys to determine execution of the MULTI/EXEC block.

        :raises TypeError: if any of arguments is None
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if any(k is None for k in keys):
            raise TypeError("keys must not be None")
        fut = self._conn.execute(b'WATCH', key, *keys)
        return wait_ok(fut)

    @property
    def multi_exec(self):
        """Executes redis commands in MULTI/EXEC block.

        Usage as follows:

        >>> yield from redis.multi_exec(
        ...     redis.incr('foo'),
        ...     redis.incr('bar'),
        ...     )
        [1, 1]

        Returns list of results.

        :raises TypeError: if any of arguments is not coroutine object.
        """
        return _MultiExec(self)


class _MultiExec:

    def __init__(self, redis):
        self.redis = redis
        self._fut = redis.multi()

    @asyncio.coroutine
    def __call__(self, *futures):
        if not len(futures):
            raise TypeError("At least one future/coroutine object is required")
        if not all(self._type_check(fut) for fut in futures):
            raise TypeError("All arguments must be coroutine"
                            " objects or Futures")
        try:
            yield from self._fut
            for fut in futures:
                yield from fut
        finally:
            if not self.redis.connection.closed:
                if self.redis.connection._transaction_error:
                    yield from self.redis.discard()
                else:
                    return (yield from self.redis.exec())

    def _type_check(self, obj):
        return asyncio.iscoroutine(obj) or isinstance(obj, asyncio.Future)


def check_errors(res):
    for obj in res:
        if isinstance(obj, RedisError):
            raise obj
    return res
