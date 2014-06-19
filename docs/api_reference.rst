:mod:`aioredis` --- API Reference
==================================

.. _aioredis-pool:

Pool Reference
--------------

.. module:: aioredis


.. _aioredis-create_pool:

.. function:: create_pool(address, db=0, password=None, \*,\
                          minsize=10, maxsize=10, commands_factory=Redis,\
                          loop=None)

   A :ref:`coroutine<coroutine>` that creates Redis connections pool.

   :param address: An address where to connect. Can be a (host, port) tuple or
                   unix domain socket path string.
   :type address: tuple or str

   :param int db: Redis database index to switch to when connected.

   :param password: Password to use if redis server instance requires
                    authorization.
   :type password: str or None

   :param int minsize: Minimum number of free connection to create in pool.
                       ``10`` by default.
   :param int maxsize: Maximum number of connection to keep in pool.
                       ``10`` by default.

   :param commands_factory: A factory to be passed to ``create_redis``
                            call. :class:`Redis` by default.
   :type commands_factory: callable

   :param loop: An optional *event loop* instance.
   :type loop: EventLoop

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

      :param conn: A RedisCommand instance.


.. _aioredis-redis:

Commands interface reference
----------------------------------

.. function:: create_redis(address, db=0, password=None, \*,\
                           commands_factory=Redis, loop=None)

   This :ref:`coroutine<coroutine>` creates high-level Redis
   interface instance.

   :param address: An address where to connect. Can be a (host, port) tuple or
                   unix domain socket path string.
   :type address: tuple or str

   :param int db: Redis database index to switch to when connected.

   :param password: Password to use if redis server instance requires
                    authorization.
   :type password: str or None

   :param commands_factory: A factory accepting single parameter --
    :class:`RedisConnection` instance and returning an object providing
    high-level interface to Redis. :class:`Redis` by default.
   :type commands_factory: callable


.. class:: Redis

   High-level Redis commands interface.


.. _aioredis-connection:

Connection Reference
--------------------


.. function:: create_connection(address, db=0, password=None, \*, loop=None)

   Creates low-level Redis connection.

   This is a :ref:`coroutine<coroutine>` function.

   :param address: An address where to connect. Can be a (host, port) tuple or
                   unix domain socket path string.
   :type address: tuple or str

   :param int db: Redis database index to switch to when connected.

   :param password: Password to use if redis server instance requires
                    authorization.
   :type password: str or None

   :return: :class:`RedisConnection` instance.


.. class:: RedisConnection

   Low-level Redis connection interface.

   .. attribute:: db

      Current database index (*read-only*).

   .. attribute:: closed

      Set to True if connection is closed (*read-only*).


   .. method:: execute(cmd, \*args):

      A :ref:`coroutine<coroutine>` function to execute Redis command.

      :param cmd: Command to execute
      :type cmd: str, bytes, bytearray

      :raise ReplyError: For redis error replies.

      :return: Returns bytes or int reply


   .. method:: close()

      Closes connection.


   .. method:: select(db)

      Changes current db index to new one.

      :param int db: New redis database index.

      :return True: Always returns True or raises exception.


   .. method:: auth(password)

      Send AUTH command.

      :param str password: Plain-text password

      :return bool: True if redis replied with 'OK'.
