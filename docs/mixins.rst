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

.. class:: Pipeline
   :module: aioredis.commands.transaction

   TBD

   .. method:: execute(\*, return_exceptions=False)

      Executes buffered commands and returns result

      :param bool return_exceptions: Raise or return exceptions.

      :raise aioredis.PipelineError:

.. class:: MultiExec
   :module: aioredis.commands.transaction

   TBD

   .. method:: execute(\*, return_exceptions=False)

      Executes buffered commands and returns result.

      :param bool return_exceptions: Raise or return exceptions.

      :raise aioredis.MultiExecError:

Scripting commands
------------------

.. autoclass:: ScriptingCommandsMixin
   :members:

Server commands
---------------

.. autoclass:: ServerCommandsMixin
   :members:
