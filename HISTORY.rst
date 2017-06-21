0.2.9 (2016-10-24)
^^^^^^^^^^^^^^^^^^

**NEW**:

* Allow multiple keys in ``EXISTS`` command
  (see `#156 <https://github.com/aio-libs/aioredis/issues/156>`_
  and `#157 <https://github.com/aio-libs/aioredis/issues/157>`_);

**FIX**:

* Close RedisPool when connection to Redis failed
  (see `#136 <https://github.com/aio-libs/aioredis/issues/136>`_);

* Add simple ``INFO`` command argument validation
  (see `#140 <https://github.com/aio-libs/aioredis/issues/140>`_);

* Remove invalid uses of ``next()``

**MISC**:

* Update devel.rst docs; update Pub/Sub Channel docs (cross-refs);

* Update MANIFEST.in to include docs, examples and tests in source bundle;


0.2.8 (2016-07-22)
^^^^^^^^^^^^^^^^^^

**NEW**:

* Add ``hmset_dict`` command
  (see `#130 <https://github.com/aio-libs/aioredis/issues/130>`_);

* Add ``RedisConnection.address`` property;

* RedisPool ``minsize``/``maxsize`` must not be ``None``;

* Implement ``close()``/``wait_closed()``/``closed`` interface for pool
  (see `#128 <https://github.com/aio-libs/aioredis/issues/128>`_);

**FIX**:

* Add test for ``hstrlen``;

* Test fixes

**MISC**:

* Enable Redis 3.2.0 on Travis;

* Add spell checking when building docs
  (see `#132 <https://github.com/aio-libs/aioredis/issues/132>`_);

* Documentation updated;


0.2.7 (2016-05-27)
^^^^^^^^^^^^^^^^^^

* ``create_pool()`` minsize default value changed to 1;

* Fixed cancellation of wait_closed
  (see `#118 <https://github.com/aio-libs/aioredis/issues/118>`_);

* Fixed ``time()`` convertion to float
  (see `#126 <https://github.com/aio-libs/aioredis/issues/126>`_);

* Fixed ``hmset()`` method to return bool instead of ``b'OK'``
  (see `#126`_);

* Fixed multi/exec + watch issue (changed watch variable was causing
  ``tr.execute()`` to fail)
  (see `#121 <https://github.com/aio-libs/aioredis/issues/121>`_);

* Replace ``asyncio.Future`` uses with utility method
  (get ready to Python 3.5.2 ``loop.create_future()``);

* Tests switched from unittest to pytest (see `#126`_);

* Documentation updates;


0.2.6 (2016-03-30)
^^^^^^^^^^^^^^^^^^

* Fixed Multi/Exec transactions cancellation issue
  (see `#110 <https://github.com/aio-libs/aioredis/issues/110>`_
  and `#114 <https://github.com/aio-libs/aioredis/issues/114>`_);

* Fixed Pub/Sub subscribe concurrency issue
  (see `#113 <https://github.com/aio-libs/aioredis/issues/113>`_
  and `#115 <https://github.com/aio-libs/aioredis/issues/115>`_);

* Add SSL/TLS support
  (see  `#116 <https://github.com/aio-libs/aioredis/issues/116>`_);

* ``aioredis.ConnectionClosedError`` raised in ``execute_pubsub`` as well
  (see `#108 <https://github.com/aio-libs/aioredis/issues/108>`_);

* ``Redis.slaveof()`` method signature changed: now to disable
  replication one should call ``redis.slaveof(None)`` instead of ``redis.slaveof()``;

* More tests added;


0.2.5 (2016-03-02)
^^^^^^^^^^^^^^^^^^

* Close all Pub/Sub channels on connection close
  (see `#88 <https://github.com/aio-libs/aioredis/issues/88>`_);

* Add ``iter()`` method to ``aioredis.Channel`` allowing to use it
  with ``async for``
  (see `#89 <https://github.com/aio-libs/aioredis/issues/89>`_);

* Inline code samples in docs made runnable and downloadable
  (see `#92 <https://github.com/aio-libs/aioredis/issues/92>`_);

* Python 3.5 examples converted to use ``async``/``await`` syntax
  (see `#93 <https://github.com/aio-libs/aioredis/issues/93>`_);

* Fix Multi/Exec to honor encoding parameter
  (see `#94 <https://github.com/aio-libs/aioredis/issues/94>`_
  and `#97 <https://github.com/aio-libs/aioredis/issues/97>`_);

* Add debug message in ``create_connection``
  (see `#90 <https://github.com/aio-libs/aioredis/issues/90>`_);

* Replace ``asyncio.async`` calls with wrapper that respects asyncio version
  (see `#101 <https://github.com/aio-libs/aioredis/issues/101>`_);

* Use NODELAY option for TCP sockets
  (see `#105 <https://github.com/aio-libs/aioredis/issues/105>`_);

* New ``aioredis.ConnectionClosedError`` exception added. Raised if
  connection to Redis server is lost
  (see `#108 <https://github.com/aio-libs/aioredis/issues/108>`_
  and `#109 <https://github.com/aio-libs/aioredis/issues/109>`_);

* Fix RedisPool to close and drop connection in subscribe mode on release;

* Fix ``aioredis.util.decode`` to recursively decode list responses;

* More examples added and docs updated;

* Add google groups link to README;

* Bump year in LICENSE and docs;


0.2.4 (2015-10-13)
^^^^^^^^^^^^^^^^^^

* Python 3.5 ``async`` support:

  - New scan commands API (``iscan``, ``izscan``, ``ihscan``);

  - Pool made awaitable (allowing ``with await pool: ...`` and ``async
    with pool.get() as conn:`` constructs);

* Fixed dropping closed connections from free pool
  (see `#83 <https://github.com/aio-libs/aioredis/issues/83>`_);

* Docs updated;


0.2.3 (2015-08-14)
^^^^^^^^^^^^^^^^^^

* Redis cluster support work in progress;

* Fixed pool issue causing pool growth over max size & ``acquire`` call hangs
  (see `#71 <https://github.com/aio-libs/aioredis/issues/71>`_);

* ``info`` server command result parsing implemented;

* Fixed behavior of util functions
  (see `#70 <https://github.com/aio-libs/aioredis/issues/70>`_);

* ``hstrlen`` command added;

* Few fixes in examples;

* Few fixes in documentation;


0.2.2 (2015-07-07)
^^^^^^^^^^^^^^^^^^

* Decoding data with ``encoding`` parameter now takes into account
  list (array) replies
  (see `#68 <https://github.com/aio-libs/aioredis/pull/68>`_);

* ``encoding`` parameter added to following commands:

  - generic commands: keys, randomkey;

  - hash commands: hgetall, hkeys, hmget, hvals;

  - list commands: blpop, brpop, brpoplpush, lindex, lpop, lrange, rpop, rpoplpush;

  - set commands: smembers, spop, srandmember;

  - string commands: getrange, getset, mget;

* Backward incompatibility:

  ``ltrim`` command now returns bool value instead of 'OK';

* Tests updated;


0.2.1 (2015-07-06)
^^^^^^^^^^^^^^^^^^

* Logging added (aioredis.log module);

* Fixed issue with ``wait_message`` in pub/sub
  (see `#66 <https://github.com/aio-libs/aioredis/issues/66>`_);


0.2.0 (2015-06-04)
^^^^^^^^^^^^^^^^^^

* Pub/Sub support added;

* Fix in ``zrevrangebyscore`` command
  (see `#62 <https://github.com/aio-libs/aioredis/pull/62>`_);

* Fixes/tests/docs;


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
