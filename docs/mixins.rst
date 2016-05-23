.. _aioredis-commands:

:class:`aioredis.Redis` --- Commands Mixins Reference
=====================================================

.. module:: aioredis.commands

This section contains reference for mixins implementing Redis commands.

Descriptions are taken from ``docstrings`` so may not contain proper markup.


.. autoclass:: aioredis.Redis
   :members:

Generic commands
----------------

.. autoclass:: GenericCommandsMixin
   :members:

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

.. class:: Pipeline(connection, commands_factory=lambda conn: conn, \*,\
                    loop=None)

   Commands pipeline.

   Buffers commands for execution in bulk.

   This class implements `__getattr__` method allowing to call methods
   on instance created with ``commands_factory``.

   :param connection: Redis connection
   :type connection: aioredis.RedisConnection

   :param callable commands_factory: Commands factory to get methods from.

   :param loop: An optional *event loop* instance
                (uses :func:`asyncio.get_event_loop` if not specified).
   :type loop: :ref:`EventLoop<asyncio-event-loop>`

   .. method:: execute(\*, return_exceptions=False)

      Executes all buffered commands and returns result.

      Any exception that is raised by any command is caught and
      raised later when processing results.

      If ``return_exceptions`` is set to ``True`` then all collected errors
      are returned in resulting list otherwise single
      :exc:`aioredis.PipelineError` exception is raised
      (containing all collected errors).

      :param bool return_exceptions: Raise or return exceptions.

      :raise aioredis.PipelineError: Raised when any command caused error.

.. class:: MultiExec(connection, commands_factory=lambda conn: conn, \*,\
                     loop=None)

   Bases: :class:`~Pipeline`.

   Multi/Exec pipeline wrapper.

   See :class:`~Pipeline` for parameters description.

   .. method:: execute(\*, return_exceptions=False)

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
