import asyncio
import functools

from ..errors import RedisError, MultiExecError
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

    def multi_exec(self):
        """Returns MULTI/EXEC pipeline wrapper.

        Usage as following:

        >>> tr = redis.multi_exec()
        >>> fut1 = tr.incr('foo')   # NO `yield from` as it will block forever
        >>> fut2 = tr.incr('bar')
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
    >>> # A)
    >>> yield from tr.execute()
    >>> res1 = yield from f1
    >>> res2 = yield from f2
    >>> # or B)
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

    def __init__(self, connection, commands_factory=lambda conn: conn,
                 *, loop=None):
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
    def execute(self, *, return_exceptions=False):
        """Executes all buffered commands."""
        assert not self._done, "Transaction already done. Create new one."
        self._done = True

        if not self._pipeline and not self._results:
            raise TypeError("At least one command within MULTI/EXEC"
                            " block is required")
        if not self._pipeline:  # error in results
            gather = asyncio.gather(*self._results, loop=self._loop,
                                    return_exceptions=return_exceptions)
            return (yield from gather)

        yield from self._conn.execute(b'MULTI')
        try:
            futures = []
            waiting = []
            for fut, cmd, args, kw in self._pipeline:
                try:
                    futures.append(self._conn.execute(cmd, *args, **kw))
                    waiting.append(fut)
                except Exception as exc:
                    fut.set_exception(exc)
            try:
                yield from asyncio.gather(*futures, loop=self._loop)
            except asyncio.CancelledError:
                pass    # some command was cancelled, we handle this in finally
        finally:
            assert len(waiting) == len(futures)
            if not self._conn.closed:
                if self._conn._transaction_error:
                    yield from self._discard(waiting)
                else:
                    return (yield from self._exec(waiting, return_exceptions))
            else:   # connection is closed
                assert len(futures) == len(waiting), (futures, waiting)
                self._cancel(futures, waiting)
    # end

    def _discard(self, waiting):
        err = self._conn._transaction_error
        yield from self._conn.execute(b'DISCARD')
        for fut in waiting:
            fut.set_exception(err)

    def _exec(self, waiting, return_exceptions):
        result = yield from self._conn.execute(b'EXEC')
        assert len(result) == len(waiting), (result, waiting)
        errors = []
        for val, fut in zip(result, waiting):
            if isinstance(val, RedisError):
                fut.set_exception(val)
                errors.append(val)
            else:
                fut.set_result(val)
        if errors and not return_exceptions:
            raise MultiExecError(errors)
        gather = asyncio.gather(
            *self._results, loop=self._loop,
            return_exceptions=return_exceptions)
        return (yield from gather)

    def _cancel(self, futures, waiting):
        for src_fut, dst_fut in zip(futures, waiting):
            if src_fut.done():
                if src_fut.cancelled():
                    dst_fut.cancel()
                elif src_fut.exception():
                    dst_fut.set_exception(src_fut.exception())
                else:
                    dst_fut.set_result(src_fut.result())
            else:
                dst_fut.cancel()


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

    # TODO: add here or remove in connection methods like `select`, `auth` etc
