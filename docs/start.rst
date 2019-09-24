.. highlight:: python3
.. module:: aioredis.commands

Getting started
===============

Installation
------------

.. code-block:: bash

   $ pip install aioredis

This will install aioredis along with its dependencies:

* hiredis protocol parser;

* async-timeout --- used in Sentinel client.

Without dependencies
~~~~~~~~~~~~~~~~~~~~

In some cases [1]_ you might need to install :mod:`aioredis` without ``hiredis``,
it is achievable with the following command:

.. code-block:: bash

   $ pip install --no-deps aioredis async-timeout

Installing latest version from Git
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   $ pip install git+https://github.com/aio-libs/aioredis@master#egg=aioredis

Connecting
----------

:download:`get source code<../examples/getting_started/00_connect.py>`

.. literalinclude:: ../examples/getting_started/00_connect.py
   :language: python3

:func:`aioredis.create_redis_pool` creates a Redis client backed by a pool of
connections. The only required argument is the address of Redis server.
Redis server address can be either host and port tuple
(ex: ``('localhost', 6379)``), or a string which will be parsed into
TCP or UNIX socket address (ex: ``'unix://var/run/redis.sock'``,
``'//var/run/redis.sock'``, ``redis://redis-host-or-ip:6379/1``).

Closing the client. Calling ``redis.close()`` and then ``redis.wait_closed()``
is strongly encouraged as this will methods will shutdown all open connections
and cleanup resources.

See the :doc:`commands reference </mixins>` for the full list of supported commands.

Connecting to specific DB
~~~~~~~~~~~~~~~~~~~~~~~~~

There are several ways you can specify database index to select on connection:

#. explicitly pass db index as ``db`` argument:

   .. code-block:: python

      redis = await aioredis.create_redis_pool(
       'redis://localhost', db=1)

#. pass db index in URI as path component:

   .. code-block:: python

      redis = await aioredis.create_redis_pool(
          'redis://localhost/2')

   .. note::

      DB index specified in URI will take precedence over
      ``db`` keyword argument.

#. call :meth:`~aioredis.Redis.select` method:

   .. code-block:: python

      redis = await aioredis.create_redis_pool(
          'redis://localhost/')
      await redis.select(3)


Connecting to password-protected Redis instance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The password can be specified either in keyword argument or in address URI:

.. code-block:: python

   redis = await aioredis.create_redis_pool(
       'redis://localhost', password='sEcRet')

   redis = await aioredis.create_redis_pool(
       'redis://:sEcRet@localhost/')

   redis = await aioredis.create_redis_pool(
       'redis://localhost/?password=sEcRet')

.. note::
   Password specified in URI will take precedence over password keyword.

   Also specifying both password as authentication component and
   query parameter in URI is forbidden.

   .. code-block:: python

      # This will cause assertion error
      await aioredis.create_redis_pool(
          'redis://:sEcRet@localhost/?password=SeCreT')

Result messages decoding
------------------------

By default :mod:`aioredis` will return :class:`bytes` for most Redis
commands that return string replies. Redis error replies are known to be
valid UTF-8 strings so error messages are decoded automatically.

If you know that data in Redis is valid string you can tell :mod:`aioredis`
to decode result by passing keyword-only argument ``encoding``
in a command call:

:download:`get source code<../examples/getting_started/01_decoding.py>`

.. literalinclude:: ../examples/getting_started/01_decoding.py
   :language: python3


:mod:`aioredis` can decode messages for all Redis data types like
lists, hashes, sorted sets, etc:

:download:`get source code<../examples/getting_started/02_decoding.py>`

.. literalinclude:: ../examples/getting_started/02_decoding.py
   :language: python3


Multi/Exec transactions
-----------------------

:download:`get source code<../examples/getting_started/03_multiexec.py>`

.. literalinclude:: ../examples/getting_started/03_multiexec.py
   :language: python3

:meth:`~TransactionsCommandsMixin.multi_exec` method creates and returns new
:class:`~aioredis.commands.MultiExec` object which is used for buffering commands and
then executing them inside MULTI/EXEC block.

.. warning::

   It is very important not to ``await`` buffered command
   (ie ``tr.set('foo', '123')``) as it will block forever.

   The following code will block forever::

      tr = redis.multi_exec()
      await tr.incr('foo')   # that's all. we've stuck!


Pub/Sub mode
------------

:mod:`aioredis` provides support for Redis Publish/Subscribe messaging.

To start listening for messages you must call either
:meth:`~PubSubCommandsMixin.subscribe` or
:meth:`~PubSubCommandsMixin.psubscribe` method.
Both methods return list of :class:`~aioredis.Channel` objects representing
subscribed channels.

Right after that the channel will receive and store messages
(the ``Channel`` object is basically a wrapper around :class:`asyncio.Queue`).
To read messages from channel you need to use :meth:`~aioredis.Channel.get`
or :meth:`~aioredis.Channel.get_json` coroutines.

Example subscribing and reading channels:

:download:`get source code<../examples/getting_started/04_pubsub.py>`

.. literalinclude:: ../examples/getting_started/04_pubsub.py
   :language: python3

Subscribing and reading patterns:

:download:`get source code<../examples/getting_started/05_pubsub.py>`

.. literalinclude:: ../examples/getting_started/05_pubsub.py
   :language: python3

Sentinel client
---------------

:download:`get source code<../examples/getting_started/06_sentinel.py>`

.. literalinclude:: ../examples/getting_started/06_sentinel.py
   :language: python3

Sentinel client requires a list of Redis Sentinel addresses to connect to
and start discovering services.

Calling :meth:`~aioredis.sentinel.SentinelPool.master_for` or
:meth:`~aioredis.sentinel.SentinelPool.slave_for` methods will return
Redis clients connected to specified services monitored by Sentinel.

Sentinel client will detect failover and reconnect Redis clients automatically.

See detailed reference :doc:`here <sentinel>`

----

.. [1]
   Celery hiredis issues
   (`#197 <https://github.com/aio-libs/aioredis/issues/197>`_,
   `#317 <https://github.com/aio-libs/aioredis/pull/317>`_)
