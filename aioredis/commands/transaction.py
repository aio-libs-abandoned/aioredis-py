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

    def pipeline(self):
        return Pipeline(self._conn, self.__class__,
                        loop=self._conn._loop)


def check_errors(res):
    for obj in res:
        if isinstance(obj, RedisError):
            raise obj
    return res


class _RedisBuffer:

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


class Pipeline:
    """Commands pipeline.

    Usage:

    >>> pipe = redis.pipeline()
    >>> fut1 = pipe.incr('foo')
    >>> fut2 = pipe.incr('bar')
    >>> yield from pipe.execute()
    [1, 1]
    >>> yield from fut1
    1
    >>> yield from fut2
    1
    """

    def __init__(self, connection, commands_factory=lambda conn: conn,
                 *, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self._conn = connection
        self._loop = loop
        self._pipeline = []
        self._results = []
        self._buffer = _RedisBuffer(self._pipeline, loop=loop)
        self._redis = commands_factory(self._buffer)
        self._done = False

    def __getattr__(self, name):
        assert not self._done, "Pipeline already executed. Create new one."
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

    def _gather_result(self, return_exceptions):
        errors = []
        results = []
        for fut in self._results:
            try:
                res = yield from fut
                results.append(res)
            except Exception as exc:
                errors.append(exc)
                results.append(exc)
        if errors and not return_exceptions:
            if len(errors) == 1:
                raise errors[0]
            else:
                raise MultiExecError(errors)
        return results

    @asyncio.coroutine
    def execute(self, *, return_exceptions=False):
        """Execute all buffered commands.

        Any exception that is raised by any command is caught and
        raised later when processing results.

        Exceptions can also be returned in result if
        `return_exceptions` flag is set to True.
        """
        assert not self._done, "Pipeline already executed. Create new one."
        self._done = True

        if self._pipeline:
            yield from asyncio.gather(*self._send_pipeline(),
                                      loop=self._loop,
                                      return_exceptions=True)

        return (yield from self._gather_result(return_exceptions))

    def _send_pipeline(self):
        for fut, cmd, args, kw in self._pipeline:
            try:
                result_fut = self._conn.execute(cmd, *args, **kw)
                result_fut.add_done_callback(
                    functools.partial(self._check_result, waiter=fut))
            except Exception as exc:
                fut.set_exception(exc)
            else:
                yield result_fut

    def _check_result(self, fut, waiter):
        if fut.cancelled():
            waiter.cancel()
        elif fut.exception():
            waiter.set_exception(fut.exception())
        else:
            self._set_result(fut, waiter)

    def _set_result(self, fut, waiter):
        waiter.set_result(fut.result())


class MultiExec(Pipeline):
    """Multi/Exec pipeline wrapper.

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

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self._waiters = []

    def execute(self, *, return_exceptions=False):
        assert not self._done, "Transaction alredy executed. Create new one."
        self._done = True

        if self._pipeline:
            yield from self._conn.execute('MULTI')
            try:
                yield from asyncio.gather(*self._send_pipeline(),
                                          loop=self._loop)
            except asyncio.CancelledError:
                pass
            finally:
                if self._conn.closed:
                    for fut in self._waiters:
                        fut.cancel()
                else:
                    if self._conn._transaction_error:
                        err = self._conn._transaction_error
                        yield from self._conn.execute('DISCARD')
                        for fut in self._waiters:
                            fut.set_exception(err)
                    else:
                        results = yield from self._conn.execute('EXEC')
                        assert len(results) == len(self._waiters)
                        errors = []
                        for val, fut in zip(results, self._waiters):
                            if isinstance(val, RedisError):
                                fut.set_exception(val)
                                errors.append(val)
                            else:
                                fut.set_result(val)
                        if errors and not return_exceptions:
                            raise MultiExecError(errors)
                        return (yield from self._gather_result(
                            return_exceptions))
        else:
            return (yield from self._gather_result(return_exceptions))

    def _set_result(self, fut, waiter):
        # fut is done and must be 'QUEUED'
        if fut in self._waiters:
            self._waiters.remove(fut)
            waiter.set_result(fut.result())
        elif fut.result() not in {b'QUEUED', 'QUEUED'}:
            waiter.set_result(fut.result())
        else:
            fut = asyncio.Future(loop=self._loop)
            self._waiters.append(fut)
            fut.add_done_callback(
                functools.partial(self._check_result, waiter=waiter))
