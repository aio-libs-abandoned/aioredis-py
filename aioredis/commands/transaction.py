import asyncio

from ..errors import RedisError


class TransactionsCommandsMixin:
    """Transaction commands mixin.

    For commands details see: http://redis.io/commands/#transactions
    """

    @asyncio.coroutine
    def discard(self):
        """Discard all commands issued after MULTI."""
        assert self._conn.in_transaction
        res = yield from self._conn.execute(b'DISCARD')
        return res == b'OK'

    @asyncio.coroutine
    def exec(self):
        """Execute all commands issued after MULTI."""
        assert self._conn.in_transaction
        res = yield from self._conn.execute(b'EXEC')
        for obj in res:
            if isinstance(obj, RedisError):
                raise obj
        return res

    @asyncio.coroutine
    def multi(self):
        """Mark the start of a transaction block."""
        assert not self._conn.in_transaction
        res = yield from self._conn.execute(b'MULTI')
        return res == b'OK'

    @asyncio.coroutine
    def unwatch(self):
        """Forget about all watched keys."""
        res = yield from self._conn.execute(b'UNWATCH')
        return res == b'OK'

    @asyncio.coroutine
    def watch(self, key, *keys):
        """Watch the given keys to determine execution of the MULTI/EXEC block.
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if any(k is None for k in keys):
            raise TypeError("keys must not be None")
        res = yield from self._conn.execute(b'WATCH', key, *keys)
        return res == b'OK'

    @asyncio.coroutine
    def multi_exec(self, *coros):
        """Executes redis commands in MULTI/EXEC block.

        Usage as follows:

        >>> yield from redis.multi_exec(
        ...     redis.incr('foo'),
        ...     redis.incr('bar'),
        ...     )
        [1, 1]

        Returns list of results.
        """
        if not len(coros):
            raise TypeError("At least one coroutine object is required")
        if not all(asyncio.iscoroutine(coro) for coro in coros):
            raise TypeError("All coroutines must be coroutine objects")

        yield from self.multi()
        try:
            # TODO: check if coro is not canceled or done
            for coro in coros:
                yield from coro
        finally:
            if not self._conn.closed:
                if self._conn._transaction_error:
                    yield from self.discard()
                else:
                    return (yield from self.exec())
