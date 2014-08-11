import asyncio
import functools

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

    >>> tr = redis.multi_exec()
    >>> try:
    ...     result_future1 = yield from tr.incr('foo')
    ...     result_future2 = yield from tr.incr('bar')
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

    def exec(self, *, return_exceptions=False):
        """Execute all commands issued after MULTI."""
        assert self._conn.in_transaction
        fut = self._conn.execute(b'EXEC')
        if return_exceptions:
            return fut
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

    # @property
    # def multi_exec(self):
    #     """Executes redis commands in MULTI/EXEC block.

    #     Usage as follows:

    #     >>> yield from redis.multi_exec(
    #     ...     redis.incr('foo'),
    #     ...     redis.incr('bar'),
    #     ...     )
    #     [1, 1]

    #     Returns list of results.

    #     .. warning::
    #        ``multi_exec`` method is a descriptor and MULTI redis command is
    #        issued on __get__. This may lead to unexpected errors if
    #        expression within parentheses raised exception.

    #        This API method may be dropped in later releases.

    #     :raises TypeError: if any of arguments is not coroutine object
    #                        or Future.
    #     """
    #     warnings.warn("MultiExec API is not stable,"
    #                   " use it on your own risk!")
    #     return _MultiExec(self)

    def multi_exec(self):
        """Returns MULTI/EXEC pipeline wrapper.

        Usage as following:

        >>> tr = redis.multi_exec()
        >>> fut1 = tr.incr('foo')   # NO `yield from`!
        >>> fut2 = tr.incr('bar')   # as it will block forever
        >>> result = yield from tr.execute()
        >>> result
        [1, 1]
        >>> yield from asyncio.gather(fut1, fut2)
        [1, 1]
        """
        return MultiExec(self._conn, self.__class__,
                         loop=self._conn._loop)


def check_errors(res):
    for obj in res:
        if isinstance(obj, RedisError):
            raise obj
    return res


class MultiExec:
    """Multi/Exec wrapper.

    Usage:

    >>> tr = redis.multi_exec()
    >>> f1 = tr.incr('foo')
    >>> f2 = tr.incr('bar')
    >>> # exec, A)
    >>> yield from tr.execute()
    >>> res1 = yield from f1
    >>> res2 = yield from f2
    >>> # exec, B)
    >>> res1, res2 = yield from tr.execute()

    and ofcourse try/except:

    >>> tr = redis.multi_exec()
    >>> f1 = tr.incr('1') # won't raise any exception (why?)
    >>> try:
    ...     res = yield from tr.execute()
    ... except RedisError:
    ...     pass
    >>> assert f1.done()
    >>> assert f1.result() is res

    >>> tr = redis.multi_exec()
    >>> wait_ok_coro = tr.mset('1')
    >>> try:
    ...     ok1 = yield from tr.execute()
    ... except RedisError:
    ...     pass # handle it
    >>> ok2 = yield from wait_ok_coro
    >>> # for this to work `wait_ok_coro` must be wrapped in Future
    """

    def __init__(self, connection, commands_factory=None, *, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self._conn = connection
        self._pipeline = []
        self._results = []
        self._loop = loop
        self._buffer = _Buffer(self._pipeline, loop=self._loop)
        self._redis = commands_factory(self._buffer)
        self._done = False

    def __getattr__(self, name):
        assert not self._done, \
            "Transaction has been already done. Create new one"
        attr = getattr(self._redis, name)
        if callable(attr):

            @functools.wraps(attr)
            def wrapper(*args, **kw):
                try:
                    task = asyncio.async(attr(*args, **kw), loop=self._loop)
                except Exception as exc:
                    task = asyncio.Future(loop=self._loop)
                    task.set_exception(exc)
                self._results.append(task)
                return task
            return wrapper

        return attr

    @asyncio.coroutine
    def execute(self, *, return_exceptions=True):
        """Executes all buffered commands."""
        assert not self._done, \
            "Transaction has been already done. Create new one"
        if not self._pipeline:
            raise TypeError("At least one command within MULTI/EXEC"
                            " block is required")
        yield from self._conn.execute(b'MULTI')
        self._done = True
        try:
            futures = []
            for fut, cmd, args, kw in self._pipeline:
                futures.append(self._conn.execute(cmd, *args, **kw))
            yield from asyncio.gather(*futures, loop=self._loop)
        finally:
            if not self._conn.closed:
                if self._conn._transaction_error:
                    yield from self._discard()
                else:
                    return (yield from self._exec(return_exceptions))

    @asyncio.coroutine
    def _discard(self):
        err = self._conn._transaction_error
        yield from self._conn.execute(b'DISCARD')
        for fut, *spam in self._pipeline:
            fut.set_exception(err)

    @asyncio.coroutine
    def _exec(self, return_exceptions):
        result = yield from self._conn.execute(b'EXEC')
        last_error = None
        for val, (fut, *spam) in zip(result, self._pipeline):
            if isinstance(val, RedisError):
                fut.set_exception(val)
                last_error = val
            else:
                fut.set_result(val)
        if last_error and not return_exceptions:
            raise last_error
        gather = asyncio.gather(*self._results, loop=self._loop,
                                return_exceptions=return_exceptions)
        return (yield from gather)


class _Buffer:

    def __init__(self, pipeline, *, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self._pipeline = pipeline
        self._loop = loop

    def execute(self, cmd, *args, **kw):
        fut = asyncio.Future(loop=self._loop)
        self._pipeline.append((fut, cmd, args, kw))
        return fut
