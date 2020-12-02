.. _aioredis-commands:

:class:`aioredis.Redis` --- Commands Mixins Reference
=====================================================

.. module:: aioredis.commands

This section contains reference for mixins implementing Redis commands.

Descriptions are taken from ``docstrings`` so may not contain proper markup.


.. autoclass:: aioredis.Redis
   :members:

   :param pool_or_conn: Can be either :class:`~aioredis.RedisConnection`
      or :class:`~aioredis.ConnectionsPool`.
   :type pool_or_conn: :class:`~aioredis.abc.AbcConnection`

Generic commands
----------------

.. autoclass:: GenericCommandsMixin
   :members:


Geo commands
------------

.. versionadded:: v0.3.0

.. autoclass:: GeoCommandsMixin
   :members:

Geo commands result wrappers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. class:: GeoPoint(longitude, latitude)

   Bases: :class:`tuple`

   Named tuple representing result returned by ``GEOPOS`` and ``GEORADIUS``
   commands.

   :param float longitude: longitude value.
   :param float latitude: latitude value.

.. class:: GeoMember(member, dist, hash, coord)

   Bases: :class:`tuple`

   Named tuple representing result returned by ``GEORADIUS`` and
   ``GEORADIUSBYMEMBER`` commands.

   :param member: Value of geo sorted set item;
   :type member: str or bytes

   :param dist: Distance in units passed to call.
                :class:`None` if ``with_dist`` was not set
                in :meth:`~GeoCommandsMixin.georadius` call.
   :type dist: None or float

   :param hash: Geo-hash represented as number.
                :class:`None` if ``with_hash``
                was not in :meth:`~GeoCommandsMixin.georadius` call.
   :type hash: None or int

   :param coord: Coordinate of geospatial index member.
                 :class:`None` if ``with_coord`` was not set
                 in :meth:`~GeoCommandsMixin.georadius` call.
   :type coord: None or GeoPoint


Strings commands
----------------

.. autoclass:: StringCommandsMixin
   :members:

Hash commands
-------------

.. autoclass:: HashCommandsMixin
   :members:

List commands
-------------

.. autoclass:: ListCommandsMixin
   :members:

Set commands
------------

.. autoclass:: SetCommandsMixin
   :members:

Sorted Set commands
-------------------

.. autoclass:: SortedSetCommandsMixin
   :members:

Server commands
---------------

.. autoclass:: ServerCommandsMixin
   :members:

HyperLogLog commands
--------------------

.. autoclass:: HyperLogLogCommandsMixin
   :members:

Transaction commands
--------------------

.. autoclass:: TransactionsCommandsMixin
   :members:

.. class:: Pipeline(connection, commands_factory=lambda conn: conn)

   Commands pipeline.

   Buffers commands for execution in bulk.

   This class implements `__getattr__` method allowing to call methods
   on instance created with ``commands_factory``.

   .. deprecated:: v1.3.1
      ``loop`` argument deprecated for Python 3.8 compatibility.

   :param connection: Redis connection
   :type connection: aioredis.RedisConnection

   :param callable commands_factory: Commands factory to get methods from.

   .. comethod:: execute(\*, return_exceptions=False)

      Executes all buffered commands and returns result.

      Any exception that is raised by any command is caught and
      raised later when processing results.

      If ``return_exceptions`` is set to ``True`` then all collected errors
      are returned in resulting list otherwise single
      :exc:`aioredis.PipelineError` exception is raised
      (containing all collected errors).

      :param bool return_exceptions: Raise or return exceptions.

      :raise aioredis.PipelineError: Raised when any command caused error.

.. class:: MultiExec(connection, commands_factory=lambda conn: conn)

   Bases: :class:`~Pipeline`.

   Multi/Exec pipeline wrapper.

   See :class:`~Pipeline` for parameters description.

   .. deprecated:: v1.3.1
      ``loop`` argument deprecated for Python 3.8 compatibility.

   .. comethod:: execute(\*, return_exceptions=False)

      Executes all buffered commands and returns result.

      see :meth:`Pipeline.execute` for details.

      :param bool return_exceptions: Raise or return exceptions.

      :raise aioredis.MultiExecError: Raised instead of :exc:`aioredis.PipelineError`
      :raise aioredis.WatchVariableError: If watched variable is changed

Scripting commands
------------------

.. autoclass:: ScriptingCommandsMixin
   :members:

Server commands
---------------

.. autoclass:: ServerCommandsMixin
   :members:


Pub/Sub commands
----------------

Also see :ref:`aioredis.Channel<aioredis-channel>`.

.. autoclass:: PubSubCommandsMixin
   :members:


Cluster commands
----------------

.. warning::
   Current release (|release|) of the library **does not support**
   `Redis Cluster`_ in a full manner.
   It provides only several API methods which may be changed in future.

.. _Redis Cluster: http://redis.io/topics/cluster-tutorial

.. ::
   .. autoclass:: ClusterCommandsMixin
      :members:


Streams commands
----------------

.. autoclass:: StreamCommandsMixin
   :members:
