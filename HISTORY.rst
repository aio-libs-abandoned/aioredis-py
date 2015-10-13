0.1.5 (2014-12-09)
^^^^^^^^^^^^^^^^^^

* AutoConnector added;

* wait_closed method added for clean connections shutdown;

* ``zscore`` command fixed;

* Test fixes;


0.1.4 (2014-09-22)
^^^^^^^^^^^^^^^^^^

* Dropped following Redis methods -- ``Redis.multi()``,
  ``Redis.exec()``, ``Redis.discard()``;

* ``Redis.multi_exec`` hack'ish property removed;

* ``Redis.multi_exec()`` method added;

* High-level commands implemented:

  * generic commands (tests);

  * transactions commands (api stabilization).

* Backward incompatibilities:

  * Following sorted set commands' API changed:

    ``zcount``, ``zrangebyscore``, ``zremrangebyscore``, ``zrevrangebyscore``;

  * set string command' API changed;



0.1.3 (2014-08-08)
^^^^^^^^^^^^^^^^^^

* RedisConnection.execute refactored to support commands pipelining
  (see `#33 <http://github.com/aio-libs/aioredis/issues/33>`_);

* Several fixes;

* WIP on transactions and commands interface;

* High-level commands implemented and tested:

  * hash commands;
  * hyperloglog commands;
  * set commands;
  * scripting commands;
  * string commands;
  * list commands;


0.1.2 (2014-07-31)
^^^^^^^^^^^^^^^^^^

* ``create_connection``, ``create_pool``, ``create_redis`` functions updated:
  db and password arguments made keyword-only
  (see `#26 <http://github.com/aio-libs/aioredis/issues/26>`_);

* Fixed transaction handling
  (see `#32 <http://github.com/aio-libs/aioredis/issues/32>`_);

* Response decoding
  (see `#16 <http://github.com/aio-libs/aioredis/issues/16>`_);


0.1.1 (2014-07-07)
^^^^^^^^^^^^^^^^^^

* Transactions support (in connection, high-level commands have some issues);
* Docs & tests updated.


0.1.0 (2014-06-24)
^^^^^^^^^^^^^^^^^^

* Initial release;
* RedisConnection implemented;
* RedisPool implemented;
* Docs for RedisConnection & RedisPool;
* WIP on high-level API.
