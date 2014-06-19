:mod:`aioredis` --- API Reference
==================================

.. _aioredis-pool:

:class:`RedisPool` Reference
----------------------------

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

   :param commands_factory: A factory to be passed in ``create_redis``

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

      This method is a :ref:`coroutine<coroutine>` function.

      :param int db: New database index.

   .. method:: acquire()

      Acquires a connection from *free pool*. Creates new connection if needed.

      This method is a :ref:`coroutine<coroutine>` function.

   .. method:: release(conn)

      Returns used connection back into pool.

      :param conn: A RedisCommand instance.
