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

   async def connect_uri():
       conn = await aioredis.create_connection(
           'redis://localhost/0')
       val = await conn.execute('GET', 'my-key')

   async def connect_tcp():
       conn = await aioredis.create_connection(
           ('localhost', 6379))
       val = await conn.execute('GET', 'my-key')

   async def connect_unixsocket():
       conn = await aioredis.create_connection(
           '/path/to/redis/socket')
       # or uri 'unix:///path/to/redis/socket?db=1'
       val = await conn.execute('GET', 'my-key')

   asyncio.get_event_loop().run_until_complete(connect_tcp())
   asyncio.get_event_loop().run_until_complete(connect_unixsocket())


.. cofunction:: create_connection(address, \*, db=0, password=None, ssl=None,\
                                  encoding=None, parser=None,\
                                  timeout=None, connection_cls=None, name=None)

   Creates Redis connection.

   .. versionchanged:: v0.3.1
      ``timeout`` argument added.

   .. versionchanged:: v1.0
      ``parser`` argument added.

   .. deprecated:: v1.3.1
      ``loop`` argument deprecated for Python 3.8 compatibility.

   :param address: An address where to connect.
      Can be one of the following:

      * a Redis URI --- ``"redis://host:6379/0?encoding=utf-8"``;
        ``"redis://:password@host:6379/0?encoding=utf-8"``;

      * a (host, port) tuple --- ``('localhost', 6379)``;

      * or a unix domain socket path string --- ``"/path/to/redis.sock"``.
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

   :param parser: Protocol parser class. Can be used to set custom protocol
      reader; expected same interface as :class:`hiredis.Reader`.
   :type parser: callable or None

   :param timeout: Max time to open a connection, otherwise
                   raise :exc:`asyncio.TimeoutError` exception.
                   ``None`` by default
   :type timeout: float greater than 0 or None

   :param connection_cls: Custom connection class. ``None`` by default.
   :type connection_cls: :class:`abc.AbcConnection` or None

   :param name: Client name to set upon connecting.
   :type name: str or None

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

         >>> ch1 = Channel('A', is_pattern=False)
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

      :return: Returns a list of subscribe/unsubscribe messages,
         ex::

            >>> await conn.execute_pubsub('subscribe', 'A', 'B')
            [[b'subscribe', b'A', 1], [b'subscribe', b'B', 2]]


   .. method:: close()

      Closes connection.

      Mark connection as closed and schedule cleanup procedure.

      All pending commands will be canceled with
      :exc:`ConnectionForcedCloseError`.


   .. method:: wait_closed()

      Coroutine waiting for connection to get closed.


   .. method:: select(db)

      Changes current db index to new one.

      :param int db: New redis database index.

      :raise TypeError: When ``db`` parameter is not int.
      :raise ValueError: When ``db`` parameter is less than 0.

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

   import aioredis

   async def sample_pool():
       pool = await aioredis.create_pool('redis://localhost')
       val = await pool.execute('get', 'my-key')


.. _aioredis-create_pool:

.. function:: create_pool(address, \*, db=0, password=None, ssl=None, \
                          encoding=None, minsize=1, maxsize=10, \
                          parser=None, \
                          create_connection_timeout=None, \
                          pool_cls=None, connection_cls=None, name=None)

   A :ref:`coroutine<coroutine>` that instantiates a pool of
   :class:`~.RedisConnection`.

   .. versionchanged:: v0.2.7
      ``minsize`` default value changed from 10 to 1.

   .. versionchanged:: v0.2.8
      Disallow arbitrary ConnectionsPool maxsize.

   .. deprecated:: v0.2.9
      *commands_factory* argument is deprecated and will be removed in *v1.0*.

   .. versionchanged:: v0.3.2
      ``create_connection_timeout`` argument added.

   .. versionchanged: v1.0
      ``commands_factory`` argument has been dropped.

   .. versionadded:: v1.0
      ``parser``, ``pool_cls`` and ``connection_cls`` arguments added.

   .. deprecated:: v1.3.1
      ``loop`` argument deprecated for Python 3.8 compatibility.

   :param address: An address where to connect.
      Can be one of the following:

      * a Redis URI --- ``"redis://host:6379/0?encoding=utf-8"``;

      * a (host, port) tuple --- ``('localhost', 6379)``;

      * or a unix domain socket path string --- ``"/path/to/redis.sock"``.
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
                       Must be greater than ``0``. ``None`` is disallowed.

   :param parser: Protocol parser class. Can be used to set custom protocol
      reader; expected same interface as :class:`hiredis.Reader`.
   :type parser: callable or None

   :param create_connection_timeout: Max time to open a connection,
      otherwise raise an :exc:`asyncio.TimeoutError`. ``None`` by default.
   :type create_connection_timeout: float greater than 0 or None

   :param pool_cls: Can be used to instantiate custom pool class.
      This argument **must be** a subclass of :class:`~aioredis.abc.AbcPool`.
   :type pool_cls: aioredis.abc.AbcPool

   :param connection_cls: Can be used to make pool instantiate custom
      connection classes. This argument **must be** a subclass of
      :class:`~aioredis.abc.AbcConnection`.
   :type connection_cls: aioredis.abc.AbcConnection

   :param name: Client name to set upon connecting.
   :type name: str or None

   :return: :class:`ConnectionsPool` instance.


.. class:: ConnectionsPool

   Bases: :class:`abc.AbcPool`

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

   .. method:: execute(command, \*args, \**kwargs)

      Execute Redis command in a free connection and return
      :class:`asyncio.Future` waiting for result.

      This method tries to pick a free connection from pool and send
      command through it at once (keeping pipelining feature provided
      by :meth:`aioredis.RedisConnection.execute`).
      If no connection is found --- returns coroutine waiting for free
      connection to execute command.

      .. versionadded:: v1.0

   .. method:: execute_pubsub(command, \*channels)

      Execute Redis (p)subscribe/(p)unsubscribe command.

      ``ConnectionsPool`` picks separate free connection for pub/sub
      and uses it until pool is closed or connection is disconnected
      (unsubscribing from all channels/pattern will leave connection
      locked for pub/sub use).

      There is no auto-reconnect for Pub/Sub connection as this will
      hide from user messages loss.

      Has similar to :meth:`execute` behavior, ie: tries to pick free
      connection from pool and switch it to pub/sub mode; or fallback
      to coroutine waiting for free connection and repeating operation.

      .. versionadded:: v1.0

   .. method:: get_connection(command, args=())

      Gets free connection from pool returning tuple of (connection, address).

      If no free connection is found -- None is returned in place of connection.

      :rtype: tuple(:class:`RedisConnection` or None, str)

      .. versionadded:: v1.0

   .. comethod:: clear()

      Closes and removes all free connections in the pool.

   .. comethod:: select(db)

      Changes db index for all free connections in the pool.

      :param int db: New database index.

   .. comethod:: acquire(command=None, args=())

      Acquires a connection from *free pool*. Creates new connection if needed.

      :param command: reserved for future.
      :param args: reserved for future.
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

.. _aioredis-exceptions:

Exceptions
----------

.. exception:: RedisError

   :Bases: :exc:`Exception`

   Base exception class for aioredis exceptions.

.. exception:: ProtocolError

   :Bases: :exc:`RedisError`

   Raised when protocol error occurs.
   When this type of exception is raised connection must be considered
   broken and must be closed.

.. exception:: ReplyError

   :Bases: :exc:`RedisError`

   Raised for Redis :term:`error replies`.

.. exception:: MaxClientsError

   :Bases: :exc:`ReplyError`

   Raised when maximum number of clients has been reached
   (Redis server configured value).

.. exception:: AuthError

   :Bases: :exc:`ReplyError`

   Raised when authentication errors occur.

.. exception:: ConnectionClosedError

   :Bases: :exc:`RedisError`

   Raised if connection to server was lost/closed.

.. exception:: ConnectionForcedCloseError

   :Bases: :exc:`ConnectionClosedError`

   Raised if connection was closed with :func:`RedisConnection.close` method.

.. exception:: PipelineError

   :Bases: :exc:`RedisError`

   Raised from :meth:`~.commands.TransactionsCommandsMixin.pipeline`
   if any pipelined command raised error.

.. exception:: MultiExecError

   :Bases: :exc:`PipelineError`

   Same as :exc:`~.PipelineError` but raised when executing multi_exec
   block.

.. exception:: WatchVariableError

   :Bases: :exc:`MultiExecError`

   Raised if watched variable changed (EXEC returns None).
   Subclass of :exc:`~.MultiExecError`.

.. exception:: ChannelClosedError

   :Bases: :exc:`RedisError`

   Raised from :meth:`aioredis.Channel.get` when Pub/Sub channel is
   unsubscribed and messages queue is empty.

.. exception:: PoolClosedError

   :Bases: :exc:`RedisError`

   Raised from :meth:`aioredis.ConnectionsPool.acquire`
   when pool is already closed.

.. exception:: ReadOnlyError

   :Bases: :exc:`RedisError`

   Raised from slave when read-only mode is enabled.

.. exception:: MasterNotFoundError

   :Bases: :exc:`RedisError`

   Raised by Sentinel client if it can not find requested master.

.. exception:: SlaveNotFoundError

   :Bases: :exc:`RedisError`

   Raised by Sentinel client if it can not find requested slave.

.. exception:: MasterReplyError

   :Bases: :exc:`RedisError`

   Raised if establishing connection to master failed with ``RedisError``,
   for instance because of required or wrong authentication.

.. exception:: SlaveReplyError

   :Bases: :exc:`RedisError`

   Raised if establishing connection to slave failed with ``RedisError``,
   for instance because of required or wrong authentication.

Exceptions Hierarchy
~~~~~~~~~~~~~~~~~~~~

.. code-block:: guess

   Exception
      RedisError
         ProtocolError
         ReplyError
            MaxClientsError
            AuthError
         PipelineError
            MultiExecError
               WatchVariableError
         ChannelClosedError
         ConnectionClosedError
            ConnectionForcedCloseError
         PoolClosedError
         ReadOnlyError
         MasterNotFoundError
         SlaveNotFoundError
         MasterReplyError
         SlaveReplyError


----

.. _aioredis-channel:

Pub/Sub Channel object
----------------------

`Channel` object is a wrapper around queue for storing received pub/sub messages.


.. class:: Channel(name, is_pattern)

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

      Return value is message received or ``None`` signifying that channel has
      been unsubscribed and no more messages will be received.

      :param str encoding: If not None used to decode resulting bytes message.

      :param callable decoder: If specified used to decode message,
                               ex. :func:`json.loads()`

      :raise aioredis.ChannelClosedError: If channel is unsubscribed and
                                          has no more messages.

   .. method:: get_json(\*, encoding="utf-8")

      Shortcut to ``get(encoding="utf-8", decoder=json.loads)``

   .. comethod:: wait_message()

      Waits for message to become available in channel
      or channel is closed (unsubscribed).

      Main idea is to use it in loops:

      >>> ch = redis.channels['channel:1']
      >>> while await ch.wait_message():
      ...     msg = await ch.get()

      :rtype: bool

   .. comethod:: iter(, \*, encoding=None, decoder=None)
      :async-for:
      :coroutine:

      Same as :meth:`~.get` method but it is a native coroutine.

      Usage example::

         >>> async for msg in ch.iter():
         ...     print(msg)

      .. versionadded:: 0.2.5
         Available for Python 3.5 only


----

.. _aioredis-redis:

Commands Interface
------------------

The library provides high-level API implementing simple interface
to Redis commands.

The usage is as simple as:

.. code:: python

   import aioredis

   # Create Redis client bound to single non-reconnecting connection.
   async def single_connection():
      redis = await aioredis.create_redis(
         'redis://localhost')
      val = await redis.get('my-key')

   # Create Redis client bound to connections pool.
   async def pool_of_connections():
      redis = await aioredis.create_redis_pool(
         'redis://localhost')
      val = await redis.get('my-key')

      # we can also use pub/sub as underlying pool
      #  has several free connections:
      ch1, ch2 = await redis.subscribe('chan:1', 'chan:2')
      # publish using free connection
      await redis.publish('chan:1', 'Hello')
      await ch1.get()

For commands reference ---
see :ref:`commands mixins reference <aioredis-commands>`.


.. cofunction:: create_redis(address, \*, db=0, password=None, ssl=None,\
                             encoding=None, commands_factory=Redis,\
                             parser=None, timeout=None,\
                             connection_cls=None)

   This :ref:`coroutine<coroutine>` creates high-level Redis
   interface instance bound to single Redis connection
   (without auto-reconnect).

   .. versionadded:: v1.0
      ``parser``, ``timeout`` and ``connection_cls`` arguments added.

   .. deprecated:: v1.3.1
      ``loop`` argument deprecated for Python 3.8 compatibility.

   See also :class:`~aioredis.RedisConnection` for parameters description.

   :param address: An address where to connect. Can be a (host, port) tuple,
                   unix domain socket path string or a Redis URI string.
   :type address: tuple or str

   :param int db: Redis database index to switch to when connected.

   :param password: Password to use if Redis server instance requires
                    authorization.
   :type password: str or bytes or None

   :param ssl: SSL context that is passed through to
               :func:`asyncio.BaseEventLoop.create_connection`.
   :type ssl: :class:`ssl.SSLContext` or True or None

   :param encoding: Codec to use for response decoding.
   :type encoding: str or None

   :param commands_factory: A factory accepting single parameter --
    object implementing :class:`~abc.AbcConnection`
    and returning an instance providing
    high-level interface to Redis. :class:`Redis` by default.
   :type commands_factory: callable

   :param parser: Protocol parser class. Can be used to set custom protocol
      reader; expected same interface as :class:`hiredis.Reader`.
   :type parser: callable or None

   :param timeout: Max time to open a connection, otherwise
                   raise :exc:`asyncio.TimeoutError` exception.
                   ``None`` by default
   :type timeout: float greater than 0 or None

   :param connection_cls: Can be used to instantiate custom
      connection class. This argument **must be** a subclass of
      :class:`~aioredis.abc.AbcConnection`.
   :type connection_cls: aioredis.abc.AbcConnection

   :returns: Redis client (result of ``commands_factory`` call),
             :class:`Redis` by default.


.. cofunction:: create_redis_pool(address, \*, db=0, password=None, ssl=None,\
                                  encoding=None, commands_factory=Redis,\
                                  minsize=1, maxsize=10,\
                                  parser=None, timeout=None,\
                                  pool_cls=None, connection_cls=None,\
                                  )

   This :ref:`coroutine<coroutine>` create high-level Redis client instance
   bound to connections pool (this allows auto-reconnect and simple pub/sub
   use).

   See also :class:`~aioredis.ConnectionsPool` for parameters description.

   .. versionchanged:: v1.0
      ``parser``, ``timeout``, ``pool_cls`` and ``connection_cls``
      arguments added.

   .. deprecated:: v1.3.1
      ``loop`` argument deprecated for Python 3.8 compatibility.

   :param address: An address where to connect. Can be a (host, port) tuple,
                   unix domain socket path string or a Redis URI string.
   :type address: tuple or str

   :param int db: Redis database index to switch to when connected.
   :param password: Password to use if Redis server instance requires
                    authorization.
   :type password: str or bytes or None

   :param ssl: SSL context that is passed through to
               :func:`asyncio.BaseEventLoop.create_connection`.
   :type ssl: :class:`ssl.SSLContext` or True or None

   :param encoding: Codec to use for response decoding.
   :type encoding: str or None

   :param commands_factory: A factory accepting single parameter --
    object implementing :class:`~abc.AbcConnection` interface
    and returning an instance providing
    high-level interface to Redis. :class:`Redis` by default.
   :type commands_factory: callable

   :param int minsize: Minimum number of connections to initialize
                       and keep in pool. Default is 1.

   :param int maxsize: Maximum number of connections that can be created
                       in pool. Default is 10.

   :param parser: Protocol parser class. Can be used to set custom protocol
      reader; expected same interface as :class:`hiredis.Reader`.
   :type parser: callable or None

   :param timeout: Max time to open a connection, otherwise
                   raise :exc:`asyncio.TimeoutError` exception.
                   ``None`` by default
   :type timeout: float greater than 0 or None

   :param pool_cls: Can be used to instantiate custom pool class.
      This argument **must be** a subclass of :class:`~aioredis.abc.AbcPool`.
   :type pool_cls: aioredis.abc.AbcPool

   :param connection_cls: Can be used to make pool instantiate custom
      connection classes. This argument **must be** a subclass of
      :class:`~aioredis.abc.AbcConnection`.
   :type connection_cls: aioredis.abc.AbcConnection

   :returns: Redis client (result of ``commands_factory`` call),
             :class:`Redis` by default.
