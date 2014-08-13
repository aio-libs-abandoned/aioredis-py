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
and then gather their results.

Simple example shows both cases::

   # No pipelining;
   def wait_each_command():
       val = yield from redis.get('foo')    # wait until `val` is available
       cnt = yield from redis.incr('bar')   # wait until `cnt` is available
       return val, cnt

   # Sending multiple commands and then gathering results
   def pipelined():
       fut1 = redis.get('foo')      # issue command and return future
       fut2 = redis.incr('bar')     # issue command and return future
       val, cnt = yield from asyncio.gather(fut1, fut2) # block until results are available
       return val, cnt


.. note::

   As as convenience :mod:`aioredis` provides
   :meth:`~TransactionsCommandsMixin.pipeline`
   method allowing to execute bulk of commands at once::

      def convenience_way():
          pipe = redis.pipeline()
          fut1 = pipe.get('foo')
          fut2 = pipe.incr('bar')
          val, cnt = yield from asyncio.gather(fut1, fut2)
          return val, cnt


Multi/Exec transactions
-----------------------

:mod:`aioredis` provides several ways for executing transactions:

* when using raw connection you can issue 'Multi'/'Exec' commands
  mannulay;

* when using :class:`aioredis.Redis` instance you can either use
  :meth:`~TransactionsCommandsMixin.multi`/
  :meth:`~TransactionsCommandsMixin.exec` methods

* or use :meth:`~TransactionsCommandsMixin.multi_exec` transaction pipeline.

The later one is described in more details.
