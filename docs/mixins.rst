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

   TBD

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

   TBD

   See :class:`~Pipeline` for parameters description.

   .. method:: execute(\*, return_exceptions=False)

      Executes all buffered commands and returns result.

      see :meth:`Pipeline.execute` for details.

      :param bool return_exceptions: Raise or return exceptions.

      :raise aioredis.MultiExecError: raised instead of :exc:`aioredis.PipelineError`

Scripting commands
------------------

.. autoclass:: ScriptingCommandsMixin
   :members:

Server commands
---------------

.. autoclass:: ServerCommandsMixin
   :members:
