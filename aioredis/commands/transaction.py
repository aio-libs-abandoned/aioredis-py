import warnings
import asyncio

from ..errors import RedisError
from ..util import wait_ok, wait_convert


class TransactionsCommandsMixin:
    """Transaction commands mixin.

    For commands details see: http://redis.io/commands/#transactions

    Transactions HOWTO:

    >>> yield from redis.multi()
    >>> try:
    ...     yield from redis.incr('foo')
    ...     yield from redis.incr('bar')
    >>> except RedisErrror:
    ...     if not redis.closed:
    ...         yield from redis.discard()
    ...     raise
    >>> else:
    ...     if not redis.closed:
    ...         result = yield from redis.exec()

    >>> tr = MultiExec(redis)   # Pipeline(redis, multi_exec=True)
    >>> try:
    ...     result_future1 = yield from tr.redis.incr('foo')
    ...     result_future2 = yield from tr.redis.incr('bar')
    >>> except RedisError:
    ...     pass
    >>> else:
    ...     yield from tr
    >>> result1 = yield from result_future1
    >>> result2 = yield from result_future2
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
        """
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

        .. warning::
           ``multi_exec`` method is a descriptor and MULTI redis command is
           issued on __get__. This may lead to unexpected errors if
           expression within parentheses raised exception.

           This API method may be dropped in later releases.

        :raises TypeError: if any of arguments is not coroutine object
                           or Future.
        """
        warnings.warn("MutliExec API is not stable, use it on your own risk!")
        return _MultiExec(self)


class _MultiExec:

    def __init__(self, redis):
        self.redis = redis
        self._fut = redis.multi()   # Bad hack

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


class MultiExec:
    """Multi/Exec wrapper.

    Usage:

    >>> tr = redis.multi_exec()
    >>> f1 = tr.redis.incr('foo')
    >>> f2 = tr.redis.incr('bar')
    >>> # exec, A)
    >>> yield from tr.execute()
    >>> res1 = yield from f1
    >>> res2 = yield from f2
    >>> # exec, B)
    >>> res1, res2 = yield from tr.execute()

    and ofcourse try/except:

    >>> tr = redis.multi_exec()
    >>> f1 = tr.redis.incr('1') # won't raise any exception (why?)
    >>> try:
    ...     res = yield from tr.execute()
    ... except RedisError:
    ...     pass
    >>> assert f1.done()
    >>> assert f1.result() is res
    """

    def __init__(self, connection, commands_factory=None):
        # self._redis = redis
        self._conn = connection
        self._pipeline = []
        self._buffer_connection = None
        self._redis = commands_factory(self._buffer_connection)

    @property
    def redis(self):
        return "Mock"

    @asyncio.coroutine
    def execute(self):
        """Executes all buffered commands."""
        yield from self._conn.execute(b'MULTI')
        try:
            for fut, command in self._pipeline:
                q = yield from self._conn.execute(*command)
                assert q == b'QUEUED'
        finally:
            if not self._conn.closed:
                if self._conn._transaction_error:
                    yield from self._conn.execute(b'DISCARD')
                else:
                    res = yield from self._conn.execute(b'EXEC')
                    for val, (fut, spam) in zip(res, self._pipeline):
                        fut.set_result(val)


class RedisMock:
    pass
