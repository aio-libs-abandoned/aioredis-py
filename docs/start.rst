.. highlight:: python
.. module:: aioredis.commands

Getting started
===============


Commands Pipelining
-------------------

Commands pipelining is built-in.

Every command is sent to transport at-once
(ofcourse if no TypeErrors/ValueErrors were raised)

When you making a call with ``yield from`` you will be waiting result,
but if you want to make several calls simply collect futures of those calls
and then gather results.

Simple example shows both cases::

   # No pipelining;
   @asyncio.coroutine
   def wait_each_command():
       val = yield from redis.get('foo')    # wait until `val` is available
       cnt = yield from redis.incr('bar')   # wait until `cnt` is available
       return val, cnt

   # Sending multiple commands and then gathering results
   @asyncio.coroutine
   def pipelined():
       fut1 = redis.get('foo')      # issue command and return future
       fut2 = redis.incr('bar')     # issue command and return future
       val, cnt = yield from asyncio.gather(fut1, fut2) # block until results are available
       return val, cnt


.. note::

   As as convenience :mod:`aioredis` provides
   :meth:`~TransactionsCommandsMixin.pipeline`
   method allowing to execute bulk of commands at once::

      @asyncio.coroutine
      def convenience_way():
          pipe = redis.pipeline()
          fut1 = pipe.get('foo')
          fut2 = pipe.incr('bar')
          result = yield from pipe.execute()
          val, cnt = yield from asyncio.gather(fut1, fut2)
          assert result == [val, cnt]
          return val, cnt


Multi/Exec transactions
-----------------------

:mod:`aioredis` provides several ways for executing transactions:

* when using raw connection you can issue 'Multi'/'Exec' commands
  manually;

* when using :class:`aioredis.Redis` instance you can either use
  :meth:`~TransactionsCommandsMixin.multi`/
  :meth:`~TransactionsCommandsMixin.exec` methods

* or use :meth:`~TransactionsCommandsMixin.multi_exec` transaction pipeline.

The later one is described in more details.

:meth:`~TransactionsCommandsMixin.multi_exec` method creates and returns new
:class:`~aioredis.commands.MultiExec` object which is used for buffering commands and
then executing them inside MULTI/EXEC block.

Here is simple example:

.. code-block:: python
   :linenos:
   :emphasize-lines: 6

    @asyncio.coroutine
    def transaction():
        tr = redis.multi_exec()
        future1 = tr.set('foo', '123')
        future2 = tr.set('bar', '321')
        result = yield from tr.execute()
        assert result == (yield from asyncio.gather(future1, future2)
        return result

As you can notice ``yield from`` is **only** used at line 6 with ``tr.execute``
and **not with** ``tr.set(...)`` calls.

.. warning::

   It is very important not to ``yield from`` buffered command
   (ie ``tr.set('foo', '123')``) as it will block forever.

   The following code will block forever::

      tr = redis.multi_exec()
      yield from tr.incr('foo')   # that's all. we've stuck!
