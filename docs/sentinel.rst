.. highlight:: python3
.. module:: aioredis.sentinel

:mod:`aioredis.sentinel` --- Sentinel Client Reference
======================================================

This section contains reference for Redis Sentinel client.

Sample usage:

.. code:: python

   import aioredis

   sentinel = await aioredis.create_sentinel(
      [('sentinel.host1', 26379), ('sentinel.host2', 26379)])

   redis = sentinel.master_for('mymaster')
   assert await redis.set('key', 'value')
   assert await redis.get('key', encoding='utf-8') == 'value'

   # redis client will reconnect/reconfigure automatically 
   #  by sentinel client instance

``RedisSentinel``
-----------------

.. corofunction:: create_sentinel(sentinels, \*, db=None, password=None,\
                                  encoding=None, minsize=1, maxsize=10,\
                                  ssl=None, parser=None,\
                                  )

   Creates Redis Sentinel client.

   .. deprecated:: v1.3.1
      ``loop`` argument deprecated for Python 3.8 compatibility.

   :param sentinels: A list of Sentinel node addresses.
   :type sentinels: list[tuple]

   :param int db: Redis database index to select for every master/slave
      connections.

   :param password: Password to use if Redis server instance requires
                    authorization.
   :type password: str or bytes or None

   :param encoding: Codec to use for response decoding.
   :type encoding: str or None

   :param int minsize: Minimum number of connections (to master or slave)
      to initialize and keep in pool. Default is 1.

   :param int maxsize: Maximum number of connections (to master or slave)
      that can be created in pool. Default is 10.

   :param ssl: SSL context that is passed through to
               :func:`asyncio.BaseEventLoop.create_connection`.
   :type ssl: :class:`ssl.SSLContext` or True or None

   :param parser: Protocol parser class. Can be used to set custom protocol
      reader; expected same interface as :class:`hiredis.Reader`.
   :type parser: callable or None

   :rtype: RedisSentinel


.. class:: RedisSentinel

   Redis Sentinel client.

   The class provides interface to Redis Sentinel commands as well as
   few methods to acquire managed Redis clients, see below.

   .. attribute:: closed

      ``True`` if client is closed.

   .. method:: master_for(name)

      Get :class:`~.Redis` client to named master.
      The client is instantiated with special connections pool which
      is controlled by :class:`SentinelPool`.
      **This method is not a coroutine.**

      :param str name: Service name.

      :rtype: aioredis.Redis

   .. method:: slave_for(name)

      Get :class:`~.Redis` client to named slave.
      The client is instantiated with special connections pool which
      is controlled by :class:`SentinelPool`.
      **This method is not a coroutine.**

      :param str name: Service name.

      :rtype: aioredis.Redis

   .. method:: execute(command, \*args, \**kwargs)

      Execute Sentinel command. Every command is prefixed with ``SENTINEL``
      automatically.

      :rtype: asyncio.Future

   .. comethod:: ping()

      Send PING to Sentinel instance.
      Currently the ping command will be sent to first sentinel in pool,
      this may change in future.

   .. method:: master(name)

      Returns a dictionary containing the specified master's state.
      Please refer to Redis documentation for more info on returned data.

      :rtype: asyncio.Future

   .. method:: master_address(name)

      Returns a ``(host, port)`` pair for the given service name.

      :rtype: asyncio.Future

   .. method:: masters()

      Returns a list of dictionaries containing all masters' states.

      :rtype: asyncio.Future

   .. method:: slaves(name)

      Returns a list of slaves for the given service name.

      :rtype: asyncio.Future

   .. method:: sentinels(name)

      Returns a list of Sentinels for the given service name.

      :rtype: asyncio.Future

   .. method:: monitor(name, ip, port, quorum)

      Add a new master to be monitored by this Sentinel.

      :param str name: Service name.
      :param str ip: New node's IP address.
      :param int port: Node's TCP port.
      :param int quorum: Sentinel quorum.

   .. method:: remove(name)

      Remove a master from Sentinel's monitoring.

      :param str name: Service name

   .. method:: set(name, option, value)

      Set Sentinel monitoring parameter for a given master.
      Please refer to Redis documentation for more info on options.

      :param str name: Master's name.
      :param str option: Monitoring option name.
      :param str value: Monitoring option value.

   .. method:: failover(name)

      Force a failover of a named master.

      :param str name: Master's name.

   .. method:: check_quorum(name)

      Check if the current Sentinel configuration is able
      to reach the quorum needed to failover a master,
      and the majority needed to authorize the failover.

      :param str name: Master's name.

   .. method:: close()

      Close all opened connections.

   .. comethod:: wait_closed()

      Wait until all connections are closed.

``SentinelPool``
----------------

.. warning::
   This API has not yet stabilized and may change in future releases.

.. cofunction:: create_sentinel_pool(sentinels, \*, db=None, password=None,\
                                     encoding=None, minsize=1, maxsize=10,\
                                     ssl=None, parser=None, loop=None)

   Creates Sentinel connections pool.


.. class:: SentinelPool

   Sentinel connections pool.

   This pool manages both sentinel connections and Redis master/slave
   connections.

   .. attribute:: closed

      ``True`` if pool and all connections are closed.

   .. method:: master_for(name)

      Returns a managed connections pool for requested service name.

      :param str name: Service name.

      :rtype: ``ManagedPool``

   .. method:: slave_for(name)

      Returns a managed connections pool for requested service name.

      :param str name: Service name.

      :rtype: ``ManagedPool``

   .. method:: execute(command, \*args, \**kwargs)

      Execute Sentinel command.

   .. comethod:: discover(timeout=0.2)

      Discover Sentinels and all monitored services within given timeout.

      This will reset internal state of this pool.

   .. comethod:: discover_master(service, timeout)

      Perform named master discovery.

      :param str service: Service name.
      :param float timeout: Operation timeout

      :rtype: aioredis.RedisConnection

   .. comethod:: discover_slave(service, timeout)

      Perform slave discovery.

      :param str service: Service name.
      :param float timeout: Operation timeout

      :rtype: aioredis.RedisConnection

   .. method:: close()

      Close all controlled connections (both to sentinel and redis).

   .. comethod:: wait_closed()

      Wait until pool gets closed.
