:mod:`aioredis` --- API Reference
=================================

.. highlight:: python3
.. module:: aioredis


.. _aioredis-connection:

Connection
----------

Redis Connection is the core function of the library.
Connection instances can be used as is or through
:ref:`pool<aioredis-pool>` or :ref:`high-level API<aioredis-redis>`.

Connection usage is as simple as:

.. code:: python

   import asyncio
   import aioredis

   async def connect_tcp():
       conn = await aioredis.create_connection(
           ('localhost', 6379))
       val = await conn.execute('GET', 'my-key')

   async def connect_unixsocket():
       conn = await aioredis.create_connection(
           '/path/to/redis/socket')
       val = await conn.execute('GET', 'my-key')

   asyncio.get_event_loop().run_until_complete(connect_tcp())
   asyncio.get_event_loop().run_until_complete(connect_unixsocket())


.. cofunction:: create_connection(address, \*, db=0, password=None, ssl=None,\
                                  encoding=None, loop=None, timeout=None)

   Creates Redis connection.

   .. versionchanged:: v0.3.1
      ``timeout`` argument added.

   :param address: An address where to connect. Can be a (host, port) tuple or
                   unix domain socket path string.
   :type address: tuple or str

   :param int db: Redis database index to switch to when connected.

   :param password: Password to use if redis server instance requires
                    authorization.
   :type password: str or None

   :param ssl: SSL context that is passed through to
               :func:`asyncio.BaseEventLoop.create_connection`.
   :type ssl: :class:`ssl.SSLContext` or True or None

   :param encoding: Codec to use for response decoding.
   :type encoding: str or None

   :param loop: An optional *event loop* instance
                (uses :func:`asyncio.get_event_loop` if not specified).
   :type loop: :ref:`EventLoop<asyncio-event-loop>`

   :param timeout: Max time used to open a connection, otherwise
                   raise `asyncio.TimeoutError` exception.
                   ``None`` by default
   :type timeout: float greater than 0 or None

   :return: :class:`RedisConnection` instance.


.. class:: RedisConnection

   Bases: :class:`abc.AbcConnection`

   Redis connection interface.

   .. attribute:: address

      Redis server address; either IP-port tuple or unix socket str (*read-only*).
      IP is either IPv4 or IPv6 depending on resolved host part in initial address.

      .. versionadded:: v0.2.8

   .. attribute:: db

      Current database index (*read-only*).

   .. attribute:: encoding

      Current codec for response decoding (*read-only*).

   .. attribute:: closed

      Set to ``True`` if connection is closed (*read-only*).

   .. attribute:: in_transaction

      Set to ``True`` when MULTI command was issued (*read-only*).

   .. attribute:: pubsub_channels

      *Read-only* dict with subscribed channels.
      Keys are bytes, values are :class:`~aioredis.Channel` instances.

   .. attribute:: pubsub_patterns

      *Read-only* dict with subscribed patterns.
      Keys are bytes, values are :class:`~aioredis.Channel` instances.

   .. attribute:: in_pubsub

      Indicates that connection is in PUB/SUB mode.
      Provides the number of subscribed channels. *Read-only*.


   .. method:: execute(command, \*args, encoding=_NOTSET)

      Execute Redis command.

      The method is **not a coroutine** itself but instead it
      writes to underlying transport and returns a :class:`asyncio.Future`
      waiting for result.

      :param command: Command to execute
      :type command: str, bytes, bytearray

      :param encoding: Keyword-only argument for overriding response decoding.
                       By default will use connection-wide encoding.
                       May be set to None to skip response decoding.
      :type encoding: str or None

      :raise TypeError: When any of arguments is None or
                        can not be encoded as bytes.
      :raise aioredis.ReplyError: For redis error replies.
      :raise aioredis.ProtocolError: When response can not be decoded
                                     and/or connection is broken.

      :return: Returns bytes or int reply (or str if encoding was set)


   .. method:: execute_pubsub(command, \*channels_or_patterns)

      Method to execute Pub/Sub commands.
      The method is not a coroutine itself but returns a :func:`asyncio.gather()`
      coroutine.
      Method also accept :class:`aioredis.Channel` instances as command
      arguments::

         >>> ch1 = Channel('A', is_pattern=False, loop=loop)
         >>> await conn.execute_pubsub('subscribe', ch1)
         [[b'subscribe', b'A', 1]]

      .. versionchanged:: v0.3
         The method accept :class:`~aioredis.Channel` instances.

      :param command: One of the following Pub/Sub commands:
                      ``subscribe``, ``unsubscribe``,
                      ``psubscribe``, ``punsubscribe``.
      :type command: str, bytes, bytearray

      :param \*channels_or_patterns: Channels or patterns to subscribe connection
                                     to or unsubscribe from.
                                     At least one channel/pattern is required.

      :return: Returns a list of subscribe/unsubscribe messages, ex:

               >>> await conn.execute_pubsub('subscribe', 'A', 'B')
               [[b'subscribe', b'A', 1], [b'subscribe', b'B', 2]]


   .. method:: close()

      Closes connection.

      Mark connection as closed and schedule cleanup procedure.


   .. method:: wait_closed()

      Coroutine waiting for connection to get closed.


   .. method:: select(db)

      Changes current db index to new one.

      :param int db: New redis database index.

      :raise TypeError: When ``db`` parameter is not int.
      :raise ValueError: When ``db`` parameter is less then 0.

      :return True: Always returns True or raises exception.


   .. method:: auth(password)

      Send AUTH command.

      :param str password: Plain-text password

      :return bool: True if redis replied with 'OK'.


----

.. _aioredis-pool:

Connections Pool
----------------

The library provides connections pool. The basic usage is as follows:

.. code:: python

   import asyncio
   import aioredis

   async def sample_pool():
       pool = await aioredis.create_pool(('localhost', 6379))
       val = await pool.execute('get', 'my-key')


.. _aioredis-create_pool:

.. function:: create_pool(address, \*, db=0, password=None, ssl=None, \
                          encoding=None, minsize=1, maxsize=10, \
                          commands_factory=_NOTSET, loop=None)

   A :ref:`coroutine<coroutine>` that instantiates a pool of
   :class:`~.RedisConnection`.

   By default it creates pool of :class:`Redis` instances, but it is
   also possible to create plain connections pool by passing
   ``lambda conn: conn`` as *commands_factory*.

   .. versionchanged:: v0.2.7
      ``minsize`` default value changed from 10 to 1.

   .. versionchanged:: v0.2.8
      Disallow arbitrary RedisPool maxsize.

   .. deprecated:: v0.2.9
      *commands_factory* argument is deprecated and will be removed in *v0.3*.

   .. versionchanged:: v0.3.1
      ``create_connection_timeout`` argument added.

   :param address: An address where to connect. Can be a (host, port) tuple or
                   unix domain socket path string.
   :type address: tuple or str

   :param int db: Redis database index to switch to when connected.

   :param password: Password to use if redis server instance requires
                    authorization.
   :type password: str or None

   :param ssl: SSL context that is passed through to
               :func:`asyncio.BaseEventLoop.create_connection`.
   :type ssl: :class:`ssl.SSLContext` or True or None

   :param encoding: Codec to use for response decoding.
   :type encoding: str or None

   :param int minsize: Minimum number of free connection to create in pool.
                       ``1`` by default.

   :param int maxsize: Maximum number of connection to keep in pool.
                       ``10`` by default.
                       Must be greater then ``0``. ``None`` is disallowed.

   :param commands_factory: A factory to be passed to ``create_redis``
                            call. :class:`Redis` by default.
                            **Deprecated** since v0.2.8
   :type commands_factory: callable

   :param loop: An optional *event loop* instance
                (uses :func:`asyncio.get_event_loop` if not specified).
   :type loop: :ref:`EventLoop<asyncio-event-loop>`

   :param create_connection_timeout: Max time used to open a connection,
                                     otherwise raise an `asyncio.TimeoutError`.
                                     ``None`` by default.
   :type create_connection_timeout: float greater than 0 or None

   :return: :class:`RedisPool` instance.


.. class:: RedisPool

   .. Bases: :class:`abc.AbcPool`

   Redis connections pool.

   .. attribute:: minsize

      A minimum size of the pool (*read-only*).

   .. attribute:: maxsize

      A maximum size of the pool (*read-only*).

   .. attribute:: size

      Current pool size --- number of free and used connections (*read-only*).

   .. attribute:: freesize

      Current number of free connections (*read-only*).

   .. attribute:: db

      Currently selected db index (*read-only*).

   .. attribute:: encoding

      Current codec for response decoding (*read-only*).

   .. attribute:: closed

      ``True`` if pool is closed.

      .. versionadded:: v0.2.8

   .. comethod:: clear()

      Closes and removes all free connections in the pool.

   .. comethod:: select(db)

      Changes db index for all free connections in the pool.

      :param int db: New database index.

   .. comethod:: acquire()

      Acquires a connection from *free pool*. Creates new connection if needed.

      :raises aioredis.PoolClosedError: if pool is already closed

   .. method:: release(conn)

      Returns used connection back into pool.

      When returned connection has db index that differs from one in pool
      the connection will be dropped.
      When queue of free connections is full the connection will be dropped.

      .. note:: This method is **not a coroutine**.

      :param aioredis.RedisConnection conn: A RedisConnection instance.

   .. method:: close()

      Close all free and in-progress connections and mark pool as closed.

      .. versionadded:: v0.2.8

   .. comethod:: wait_closed()

      Wait until pool gets closed (when all connections are closed).

      .. versionadded:: v0.2.8


----

.. _aioredis-channel:

Pub/Sub Channel object
----------------------

`Channel` object is a wrapper around queue for storing received pub/sub messages.


.. class:: Channel(name, is_pattern, loop=None)

   Bases: :class:`abc.AbcChannel`

   Object representing Pub/Sub messages queue.
   It's basically a wrapper around :class:`asyncio.Queue`.

   .. attribute:: name

      Holds encoded channel/pattern name.

   .. attribute:: is_pattern

      Set to True for pattern channels.

   .. attribute:: is_active

      Set to True if there are messages in queue and connection is still
      subscribed to this channel.

   .. comethod:: get(\*, encoding=None, decoder=None)

      Coroutine that waits for and returns a message.

      Return value is message received or None signifying that channel has
      been unsubscribed and no more messages will be received.

      :param str encoding: If not None used to decode resulting bytes message.

      :param callable decoder: If specified used to decode message,
                               ex. :func:`json.loads()`

      :raise aioredis.ChannelClosedError: If channel is unsubscribed and
                                          has no more messages.

   .. method:: get_json(\*, encoding="utf-8")

      Shortcut to ``get(encoding="utf-8", decoder=json.loads)``

   .. comethod:: wait_message()

      Waits for message to become available in channel.

      Main idea is to use it in loops:

      >>> ch = redis.channels['channel:1']
      >>> while await ch.wait_message():
      ...     msg = await ch.get()

   .. comethod:: iter()
      :async-for:
      :coroutine:

      Same as :meth:`~.get` method but it is a native coroutine.

      Usage example::

         >>> async for msg in ch.iter():
         ...     print(msg)

      .. versionadded:: 0.2.5
         Available for Python 3.5 only

----

.. _aioredis-exceptions:

Exceptions
----------

.. exception:: RedisError

   Base exception class for aioredis exceptions.

.. exception:: ProtocolError

   Raised when protocol error occurs.
   When this type of exception is raised connection must be considered
   broken and must be closed.

.. exception:: ReplyError

   Raised for Redis :term:`error replies`.

.. exception:: ConnectionClosedError

   Raised if connection to server was lost/closed.

.. exception:: PipelineError

   Raised from :meth:`~.commands.TransactionsCommandsMixin.pipeline`
   if any pipelined command raised error.

.. exception:: MultiExecError

   Same as :exc:`~.PipelineError` but raised when executing multi_exec
   block.

.. exception:: WatchVariableError

   Raised if watched variable changed (EXEC returns None).
   Subclass of :exc:`~.MultiExecError`.

.. exception:: ChannelClosedError

   Raised from :meth:`aioredis.Channel.get` when Pub/Sub channel is
   unsubscribed and messages queue is empty.

.. exception:: PoolClosedError

   Raised from :meth:`aioredis.RedisPool.acquire` when pool is already closed.


Exceptions Hierarchy
~~~~~~~~~~~~~~~~~~~~

.. code-block:: guess

   Exception
      RedisError
         ProtocolError
         ReplyError
            PipelineError
               MultiExecError
                  WatchVariableError
         ChannelClosedError
         ConnectionClosedError
         PoolClosedError

----

.. _aioredis-redis:

Commands Interface
------------------

The library provides high-level API implementing simple interface
to Redis commands.

.. cofunction:: create_redis(address, \*, db=0, password=None, ssl=None,\
                             encoding=None, commands_factory=Redis,\
                             loop=None)

   This :ref:`coroutine<coroutine>` creates high-level Redis
   interface instance.

   :param address: An address where to connect. Can be a (host, port) tuple or
                   unix domain socket path string.
   :type address: tuple or str

   :param int db: Redis database index to switch to when connected.

   :param password: Password to use if redis server instance requires
                    authorization.
   :type password: str or None

   :param ssl: SSL context that is passed through to
               :func:`asyncio.BaseEventLoop.create_connection`.
   :type ssl: :class:`ssl.SSLContext` or True or None

   :param encoding: Codec to use for response decoding.
   :type encoding: str or None

   :param commands_factory: A factory accepting single parameter --
    :class:`RedisConnection` instance and returning an object providing
    high-level interface to Redis. :class:`Redis` by default.
   :type commands_factory: callable

   :param loop: An optional *event loop* instance
                (uses :func:`asyncio.get_event_loop` if not specified).
   :type loop: :ref:`EventLoop<asyncio-event-loop>`


.. NOTE: mark as deprecated
.. cofunction:: create_reconnecting_redis(address, \*, db=0, password=None,\
                           ssl=None, encoding=None, commands_factory=Redis,\
                           loop=None)

   Like :func:`create_redis` this :ref:`coroutine<coroutine>` creates
   high-level Redis interface instance that may reconnect to redis server
   between requests.  Accepts same arguments as :func:`create_redis`.

   The reconnect process is done at most once, at the start of the request. So
   if your request is broken in the middle of sending or receiving reply, it
   will not be repeated but an exception is raised.

   .. note:: There are two important differences between :func:`create_redis`
      and :func:`create_reconnecting_redis`:

      1. The :func:`create_reconnecting_redis` does not establish connection
         "right now", it defers connection establishing to the first request.

      2. Methods of :func:`Redis` factory returned do not buffer commands
         until you `yield from` it. I.e. they are real coroutines not the
         functions returning future. It may impact your pipelining.


.. class:: Redis(connection)
   :noindex:

   High-level Redis commands interface.

   For details see :ref:`mixins<aioredis-commands>` reference.
