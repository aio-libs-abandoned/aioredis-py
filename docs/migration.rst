Migrating from v0.3 to v1.0
===========================

.. contents:: API changes and backward incompatible changes:
   :local:

----

aioredis.create_pool
--------------------

:func:`~aioredis.create_pool` now returns :class:`~aioredis.ConnectionsPool`
instead of ``RedisPool``.

This means that pool now operates with :class:`~aioredis.RedisConnection`
objects and not :class:`~aioredis.Redis`.

+--------+--------------------------------------------------------------------+
|        |  .. code-block:: python3                                           |
| v0.3   |     :emphasize-lines: 5                                            |
|        |                                                                    |
|        |     pool = await aioredis.create_pool(('localhost', 6379))         |
|        |                                                                    |
|        |     with await pool as redis:                                      |
|        |         # calling methods of Redis class                           |
|        |         await redis.lpush('list-key', 'item1', 'item2')            |
|        |                                                                    |
+--------+--------------------------------------------------------------------+
|        |  .. code-block:: python3                                           |
| v1.0   |     :emphasize-lines: 5                                            |
|        |                                                                    |
|        |     pool = await aioredis.create_pool(('localhost', 6379))         |
|        |                                                                    |
|        |     with await pool as conn:                                       |
|        |         # calling conn.lpush will raise AttributeError exception   |
|        |         await conn.execute('lpush', 'list-key', 'item1', 'item2')  |
|        |                                                                    |
+--------+--------------------------------------------------------------------+


aioredis.create_reconnecting_redis
----------------------------------

:func:`~aioredis.create_reconnecting_redis` has been dropped.

:func:`~aioredis.create_redis_pool` can be used instead of former function.

+--------+--------------------------------------------------------------------+
|        |  .. code-block:: python3                                           |
| v0.3   |     :emphasize-lines: 1                                            |
|        |                                                                    |
|        |     redis = await aioredis.create_reconnecting_redis(              |
|        |         ('localhost', 6379))                                       |
|        |                                                                    |
|        |     await redis.lpush('list-key', 'item1', 'item2')                |
|        |                                                                    |
+--------+--------------------------------------------------------------------+
|        |  .. code-block:: python3                                           |
| v1.0   |     :emphasize-lines: 1                                            |
|        |                                                                    |
|        |     redis = await aioredis.create_redis_pool(                      |
|        |         ('localhost', 6379))                                       |
|        |                                                                    |
|        |     await redis.lpush('list-key', 'item1', 'item2')                |
|        |                                                                    |
+--------+--------------------------------------------------------------------+

``create_redis_pool`` returns :class:`~aioredis.Redis` initialized with
``ConnectionsPool`` which is responsible for reconnecting to server.

Also ``create_reconnecting_redis`` was patching the ``RedisConnection`` and
breaking ``closed`` property (it was always ``True``).


aioredis.Redis
--------------

:class:`~aioredis.Redis` class now operates with objects implementing
:class:`aioredis.abc.AbcConnection` interface.
:class:`~aioredis.RedisConnection` and :class:`~aioredis.ConnectionsPool` are
both implementing ``AbcConnection`` so it is become possible to use same API
when working with either single connection or connections pool.

+--------+--------------------------------------------------------------------+
|        |  .. code-block:: python3                                           |
| v0.3   |     :emphasize-lines: 5                                            |
|        |                                                                    |
|        |     redis = await aioredis.create_redis(('localhost', 6379))       |
|        |     await redis.lpush('list-key', 'item1', 'item2')                |
|        |                                                                    |
|        |     pool = await aioredis.create_pool(('localhost', 6379))         |
|        |     redis = await pool.acquire()  # get Redis object               |
|        |     await redis.lpush('list-key', 'item1', 'item2')                |
|        |                                                                    |
+--------+--------------------------------------------------------------------+
|        |  .. code-block:: python3                                           |
| v1.0   |     :emphasize-lines: 2,5                                          |
|        |                                                                    |
|        |     redis = await aioredis.create_redis(('localhost', 6379))       |
|        |     await redis.lpush('list-key', 'item1', 'item2')                |
|        |                                                                    |
|        |     redis = await aioredis.create_redis_pool(('localhost', 6379))  |
|        |     await redis.lpush('list-key', 'item1', 'item2')                |
|        |                                                                    |
+--------+--------------------------------------------------------------------+

Blocking operations and connection sharing
------------------------------------------

Current implementation of ``ConnectionsPool`` by default **execute
every command on random connection**. The *Pros* of this is that it allowed
implementing ``AbcConnection`` interface and hide pool inside ``Redis`` class,
and also keep pipelining feature (like RedisConnection.execute).
The *Cons* of this is that **different tasks may use same connection and block
it** with some long-running command.

We can call it **Shared Mode** --- commands are sent to random connections
in pool without need to lock [connection]:

.. code-block:: python3

   redis = await aioredis.create_redis_pool(
       ('localhost', 6379),
       minsize=1,
       maxsize=1)

   async def task():
       # Shared mode
       await redis.set('key', 'val')

   asyncio.ensure_future(task())
   asyncio.ensure_future(task())
   # Both tasks will send commands through same connection
   # without acquiring (locking) it first.

Blocking operations (like ``blpop``, ``brpop`` or long-running LUA scripts)
in **shared mode** mode will block connection and thus may lead to whole
program malfunction.

This *blocking* issue can be easily solved by using exclusive connection
for such operations:

.. code-block:: python3
   :emphasize-lines: 8

   redis = await aioredis.create_redis_pool(
       ('localhost', 6379),
       minsize=1,
       maxsize=1)

   async def task():
      # Exclusive mode
      with await redis as r:
          await r.set('key', 'val')
   asyncio.ensure_future(task())
   asyncio.ensure_future(task())
   # Both tasks will first acquire connection.

We can call this **Exclusive Mode** --- context manager is used to
acquire (lock) exclusive connection from pool and send all commands through it.

.. note:: This technique is similar to v0.3 pool usage:

   .. code-block:: python3

      # in aioredis v0.3
      pool = await aioredis.create_pool(('localhost', 6379))
      with await pool as redis:
          # Redis is bound to exclusive connection
          redis.set('key', 'val')


Sorted set commands return values
---------------------------------

Sorted set commands (like ``zrange``, ``zrevrange`` and others) that accept
``withscores`` argument now **return list of tuples** instead of plain list.

+--------+--------------------------------------------------------------------+
|        |  .. code-block:: python3                                           |
| v0.3   |     :emphasize-lines: 4,7-8                                        |
|        |                                                                    |
|        |     redis = await aioredis.create_redis(('localhost', 6379))       |
|        |     await redis.zadd('zset-key', 1, 'one', 2, 'two')               |
|        |     res = await redis.zrange('zset-key', withscores=True)          |
|        |     assert res == [b'one', 1, b'two', 2]                           |
|        |                                                                    |
|        |     # not an easy way to make a dict                               |
|        |     it = iter(res)                                                 |
|        |     assert dict(zip(it, it)) == {b'one': 1, b'two': 2}             |
|        |                                                                    |
+--------+--------------------------------------------------------------------+
|        |  .. code-block:: python3                                           |
| v1.0   |     :emphasize-lines: 4,7                                          |
|        |                                                                    |
|        |     redis = await aioredis.create_redis(('localhost', 6379))       |
|        |     await redis.zadd('zset-key', 1, 'one', 2, 'two')               |
|        |     res = await redis.zrange('zset-key', withscores=True)          |
|        |     assert res == [(b'one', 1), (b'two', 2)]                       |
|        |                                                                    |
|        |     # now its easier to make a dict of it                          |
|        |     assert dict(res) == {b'one': 1, b'two': 2}                     |
|        |                                                                    |
+--------+--------------------------------------------------------------------+


Hash ``hscan`` command now returns list of tuples
-------------------------------------------------

``hscan`` updated to return a list of tuples instead of plain
mixed key/value list.

+--------+--------------------------------------------------------------------+
|        |  .. code-block:: python3                                           |
| v0.3   |     :emphasize-lines: 4,7-8                                        |
|        |                                                                    |
|        |     redis = await aioredis.create_redis(('localhost', 6379))       |
|        |     await redis.hmset('hash', 'one', 1, 'two', 2)                  |
|        |     cur, data = await redis.hscan('hash')                          |
|        |     assert data == [b'one', b'1', b'two', b'2']                    |
|        |                                                                    |
|        |     # not an easy way to make a dict                               |
|        |     it = iter(data)                                                |
|        |     assert dict(zip(it, it)) == {b'one': b'1', b'two': b'2'}       |
|        |                                                                    |
+--------+--------------------------------------------------------------------+
|        |  .. code-block:: python3                                           |
| v1.0   |     :emphasize-lines: 4,7                                          |
|        |                                                                    |
|        |     redis = await aioredis.create_redis(('localhost', 6379))       |
|        |     await redis.hmset('hash', 'one', 1, 'two', 2)                  |
|        |     cur, data = await redis.hscan('hash')                          |
|        |     assert data == [(b'one', b'1'), (b'two', b'2')]                |
|        |                                                                    |
|        |     # now its easier to make a dict of it                          |
|        |     assert dict(data) == {b'one': b'1': b'two': b'2'}              |
|        |                                                                    |
+--------+--------------------------------------------------------------------+
