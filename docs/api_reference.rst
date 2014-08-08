:mod:`aioredis` --- API Reference
=================================

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

   @asyncio.coroutine
   def connection_example():
       conn = yield from aioredis.create_connection(
           ('localhost', 6379))
       # connecting to socket
       # conn = yiled from aioredis.create_connection(
       #     '/path/to/redis/socket')
       val = yield from conn.execute('GET', 'my-key')

   asyncio.get_event_loop().run_until_complete(connection_example())


.. function:: create_connection(address, \*, db=0, password=None,\
                                encoding=None, loop=None)

   Creates Redis connection.

   This is a :ref:`coroutine<coroutine>` function.

   :param address: An address where to connect. Can be a (host, port) tuple or
                   unix domain socket path string.
   :type address: tuple or str

   :param int db: Redis database index to switch to when connected.

   :param password: Password to use if redis server instance requires
                    authorization.
   :type password: str or None

   :param encoding: Codec to use for response decoding.
   :type encoding: str or None

   :param loop: An optional *event loop* instance
                (uses :func:`asyncio.get_event_loop` if not specified).
   :type loop: :ref:`EventLoop<asyncio-event-loop>`

   :return: :class:`RedisConnection` instance.


.. class:: RedisConnection

   Redis connection interface.

   .. attribute:: db

      Current database index (*read-only*).

   .. attribute:: encoding

      Current codec for response decoding (*read-only*).

   .. attribute:: closed

      Set to True if connection is closed (*read-only*).


   .. method:: execute(command, \*args, encoding=_NOTSET)

      A :ref:`coroutine<coroutine>` function to execute Redis command.

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


   .. method:: close()

      Closes connection.


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

Pool
----

The library provides connections pool. The basic usage is as follows:

.. code:: python

   import asyncio
   import aioredis

   @asyncio.coroutine
   def test_pool():
       pool = yield from aioredis.create_pool(('localhost', 6379))
       with (yield from pool) as redis:
           val = yield from redis.get('my-key')


.. _aioredis-create_pool:

.. function:: create_pool(address, \*, db=0, password=None, encoding=None, \
                          minsize=10, maxsize=10, commands_factory=Redis,\
                          loop=None)

   A :ref:`coroutine<coroutine>` that creates Redis connections pool.

   By default it creates pool of *commands_factory* instances, but it is
   also possible to create plain connections pool by passing
   ``lambda conn: conn`` as *commands_factory*.

   :param address: An address where to connect. Can be a (host, port) tuple or
                   unix domain socket path string.
   :type address: tuple or str

   :param int db: Redis database index to switch to when connected.

   :param password: Password to use if redis server instance requires
                    authorization.
   :type password: str or None

   :param encoding: Codec to use for response decoding.
   :type encoding: str or None

   :param int minsize: Minimum number of free connection to create in pool.
                       ``10`` by default.
   :param int maxsize: Maximum number of connection to keep in pool.
                       ``10`` by default.

   :param commands_factory: A factory to be passed to ``create_redis``
                            call. :class:`Redis` by default.
   :type commands_factory: callable

   :param loop: An optional *event loop* instance
                (uses :func:`asyncio.get_event_loop` if not specified).
   :type loop: :ref:`EventLoop<asyncio-event-loop>`

   :return: :class:`RedisPool` instance.


.. class:: RedisPool

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

   .. method:: clear()

      Closes and removes all free connections in the pool.

   .. method:: select(db)

      Changes db index for all free connections in the pool.

      This method is a :ref:`coroutine<coroutine>`.

      :param int db: New database index.

   .. method:: acquire()

      Acquires a connection from *free pool*. Creates new connection if needed.

      This method is a :ref:`coroutine<coroutine>`.

   .. method:: release(conn)

      Returns used connection back into pool.

      When returned connection has db index that differs from one in pool
      the connection will be dropped.
      When queue of free connections is full the connection will be dropped.

      .. note:: This method is NOT a coroutine.

      :param conn: A RedisCommand instance.


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

----

.. _aioredis-redis:

Commands Interface
------------------

The library provides high-level API implementing simple interface
to Redis commands.

.. function:: create_redis(address, \*, db=0, password=None,\
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

   :param encoding: Codec to use for response decoding.
   :type encoding: str or None

   :param commands_factory: A factory accepting single parameter --
    :class:`RedisConnection` instance and returning an object providing
    high-level interface to Redis. :class:`Redis` by default.
   :type commands_factory: callable

   :param loop: An optional *event loop* instance
                (uses :func:`asyncio.get_event_loop` if not specified).
   :type loop: :ref:`EventLoop<asyncio-event-loop>`


.. class:: Redis(connection)
   :noindex:

   High-level Redis commands interface.

   For details see :ref:`mixins<aioredis-commands>` reference.

..
    https://github.com/aio-libs/aioredis/tree/master/aioredis/commands
