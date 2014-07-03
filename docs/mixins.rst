.. _aioredis-commands:

Commands Mixins Reference
=========================

.. module:: aioredis.commands

This section contains reference for mixins implementing Redis commands.

Descriptions are taken from ``docstrings`` so may not contain proper markup.

.. autoclass:: aioredis.Redis
   :members:
   :noindex:

Generic commands
----------------

.. autoclass:: GenericCommandsMixin
   :members:

..
    Strings commands
    ----------------
    .. automodule:: aioredis.commands.string
       :members:

Set commands
------------

.. autoclass:: SetCommandsMixin
   :members:

HyperLogLog commands
--------------------

.. autoclass:: HyperLogLogCommandsMixin
   :members:
