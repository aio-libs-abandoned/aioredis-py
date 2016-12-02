import asyncio
import functools

from ..errors import RedisError, PipelineError, MultiExecError
from ..util import wait_ok, async_task, create_future


class TransactionsCommandsMixin:
    """Transaction commands mixin.

    For commands details see: http://redis.io/commands/#transactions

    Transactions HOWTO:

    >>> tr = redis.multi_exec()
    >>> result_future1 = tr.incr('foo')
    >>> result_future2 = tr.incr('bar')
    >>> try:
    ...     result = await tr.execute()
    ... except MultiExecError:
    ...     pass    # check what happened
    >>> result1 = await result_future1
    >>> result2 = await result_future2
    >>> assert result == [result1, result2]
    """

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

        Usage:

        >>> tr = redis.multi_exec()
        >>> fut1 = tr.incr('foo')   # NO `await` as it will block forever!
        >>> fut2 = tr.incr('bar')
        >>> result = await tr.execute()
        >>> result
        [1, 1]
        >>> await asyncio.gather(fut1, fut2)
        [1, 1]
        """
        return MultiExec(self._conn, self.__class__,
                         loop=self._conn._loop)

    def pipeline(self):
        """Returns :class:`Pipeline` object to execute bulk of commands.

        It is provided for convenience.
        Commands can be pipelined without it.

        Example:

        >>> pipe = redis.pipeline()
        >>> fut1 = pipe.incr('foo') # NO `await` as it will block forever!
        >>> fut2 = pipe.incr('bar')
        >>> result = await pipe.execute()
        >>> result
        [1, 1]
        >>> await asyncio.gather(fut1, fut2)
        [1, 1]
        >>> #
        >>> # The same can be done without pipeline:
        >>> #
        >>> fut1 = redis.incr('foo')    # the 'INCRY foo' command already sent
        >>> fut2 = redis.incr('bar')
        >>> await asyncio.gather(fut1, fut2)
        [2, 2]
        """
        return Pipeline(self._conn, self.__class__,
                        loop=self._conn._loop)


class _RedisBuffer:

    def __init__(self, pipeline, *, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self._pipeline = pipeline
        self._loop = loop

    def execute(self, cmd, *args, **kw):
        fut = create_future(loop=self._loop)
        self._pipeline.append((fut, cmd, args, kw))
        return fut

    # TODO: add here or remove in connection methods like `select`, `auth` etc


class Pipeline:
    """Commands pipeline.

    Usage:

    >>> pipe = redis.pipeline()
    >>> fut1 = pipe.incr('foo')
    >>> fut2 = pipe.incr('bar')
    >>> await pipe.execute()
    [1, 1]
    >>> await fut1
    1
    >>> await fut2
    1
    """
    error_class = PipelineError

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
                    task = async_task(attr(*args, **kw), loop=self._loop)
                except Exception as exc:
                    task = create_future(loop=self._loop)
                    task.set_exception(exc)
                self._results.append(task)
                return task
            return wrapper
        return attr

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
            return self._do_execute(return_exceptions=return_exceptions)
        else:
            return self._gather_result(return_exceptions)

    @asyncio.coroutine
    def _do_execute(self, *, return_exceptions=False):
        conn = yield from self._conn.get_atomic_connection()
        yield from asyncio.gather(*self._send_pipeline(conn),
                                  loop=self._loop,
                                  return_exceptions=True)
        return (yield from self._gather_result(return_exceptions))

    @asyncio.coroutine
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
            raise self.error_class(errors)
        return results

    def _send_pipeline(self, conn):
        for fut, cmd, args, kw in self._pipeline:
            try:
                result_fut = conn.execute(cmd, *args, **kw)
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
            waiter.set_result(fut.result())


class MultiExec(Pipeline):
    """Multi/Exec pipeline wrapper.

    Usage:

    >>> tr = redis.multi_exec()
    >>> f1 = tr.incr('foo')
    >>> f2 = tr.incr('bar')
    >>> # A)
    >>> await tr.execute()
    >>> res1 = await f1
    >>> res2 = await f2
    >>> # or B)
    >>> res1, res2 = await tr.execute()

    and ofcourse try/except:

    >>> tr = redis.multi_exec()
    >>> f1 = tr.incr('1') # won't raise any exception (why?)
    >>> try:
    ...     res = await tr.execute()
    ... except RedisError:
    ...     pass
    >>> assert f1.done()
    >>> assert f1.result() is res

    >>> tr = redis.multi_exec()
    >>> wait_ok_coro = tr.mset('1')
    >>> try:
    ...     ok1 = await tr.execute()
    ... except RedisError:
    ...     pass # handle it
    >>> ok2 = await wait_ok_coro
    >>> # for this to work `wait_ok_coro` must be wrapped in Future
    """
    error_class = MultiExecError

    @asyncio.coroutine
    def _do_execute(self, *, return_exceptions=False):
        self._waiters = waiters = []
        conn = yield from self._conn.get_atomic_connection()
        multi = conn.execute('MULTI')
        coros = list(self._send_pipeline(conn))
        exec_ = conn.execute('EXEC')
        gather = asyncio.gather(multi, *coros, loop=self._loop,
                                return_exceptions=True)
        try:
            yield from asyncio.shield(gather, loop=self._loop)
        except asyncio.CancelledError:
            yield from gather
        finally:
            if conn.closed:
                for fut in waiters:
                    fut.cancel()
                for fut in self._results:
                    if not fut.done():
                        fut.cancel()
            else:
                try:
                    results = yield from exec_
                except RedisError as err:
                    for fut in waiters:
                        fut.set_exception(err)
                else:
                    assert len(results) == len(waiters), (
                        "Results does not match waiters", results, waiters)
                    self._resolve_waiters(results, return_exceptions)
            return (yield from self._gather_result(return_exceptions))

    def _resolve_waiters(self, results, return_exceptions):
        errors = []
        for val, fut in zip(results, self._waiters):
            if isinstance(val, RedisError):
                fut.set_exception(val)
                errors.append(val)
            else:
                fut.set_result(val)
        if errors and not return_exceptions:
            raise MultiExecError(errors)

    def _check_result(self, fut, waiter):
        assert waiter not in self._waiters, (fut, waiter, self._waiters)
        assert not waiter.done(), waiter
        if fut.cancelled():     # yield from gather was cancelled
            waiter.cancel()
        elif fut.exception():   # server replied with error
            waiter.set_exception(fut.exception())
        elif fut.result() in {b'QUEUED', 'QUEUED'}:
            # got result, it should be QUEUED
            self._waiters.append(waiter)
