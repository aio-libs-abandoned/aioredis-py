:mod:`aioredis.sentinel` module
===============================

.. highlight:: python3
.. module:: aioredis.sentinel


Module provides Sentinel support (yet experimental).

Sample usage:

.. code:: python

   import aioredis

   sentinel = await aioredis.create_sentinel(
      [('sentinel1', 26379), ('sentinelN', 26379)])

   redis = sentinel.master_for('mymaster')
   assert await redis.set('key', 'value')
   assert await redis.get('key', encoding='utf-8') == 'value'

   # redis client will reconnect/reconfigure automatically 
   #  by sentinel client instance


.. autofunction:: create_sentinel


.. autoclass:: RedisSentinel
   :members:
