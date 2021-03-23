

# Changelog

## 2.0.0 - (2021-03-18)

### Features

- Port redis-py's client implementation to aioredis.  
  (see #891)

- Make hiredis an optional dependency.  
  (see #917)


## 1.3.1 (2019-12-02)

### Bugfixes

- Fix transaction data decoding  
  (see #657)
- Fix duplicate calls to `pool.wait_closed()` upon `create_pool()` exception.  
  (see #671)

### Deprecations and Removals

- Drop explicit loop requirement in API.
  Deprecate `loop` argument.
  Throw warning in Python 3.8+ if explicit `loop` is passed to methods.  
  (see #666)

### Misc

- (#643, #646, #648)

## 1.3.0 (2019-09-24)

### Features

- Added `xdel` and `xtrim` method which missed in `commands/streams.py` & also added unit test code for them  
  (see #438)
- Add `count` argument to `spop` command  
  (see #485)
- Add support for `zpopmax` and `zpopmin` redis commands  
  (see #550)
- Add `towncrier`: change notes are now stored in `CHANGES.txt`  
  (see #576)
- Type hints for the library  
  (see #584)
- A few additions to the sorted set commands:
- the blocking pop commands: `BZPOPMAX` and `BZPOPMIN`
- the `CH` and `INCR` options of the `ZADD` command  
  (see #618)
- Added `no_ack` parameter to `xread_group` streams method in `commands/streams.py`  
  (see #625)

### Bugfixes

- Fix for sensitive logging  
  (see #459)
- Fix slow memory leak in `wait_closed` implementation  
  (see #498)
- Fix handling of instances were Redis returns null fields for a stream message (see
  \#605)

### Improved Documentation

- Rewrite "Getting started" documentation.  
  (see #641)

### Misc

- #585,
  #611,
  #612,
  #619,
  #620,
  #642)

## 1.2.0 (2018-10-24)


### Features

- Implemented new Stream command support  
  (see #299)
- Reduce `encode_command()` cost about 60%  
  (see #397)

### Bugfixes

- Fix pipeline commands buffering was causing multiple `sendto` syscalls  
  (see #464)
  and  #473)
- Python 3.7 compatibility fixes  
  (see #426)
- Fix typos in documentation  
  (see #400)
- Fix `INFO` command result parsing  
  (see #405)
- Fix bug in `ConnectionsPool._drop_closed` method  
  (see #461)

### Miscellaneous

- Update dependencies versions
- Multiple tests improvements
## 1.1.0 (2018-02-16)


### Features

- Implement new commands: `wait`, `touch`, `swapdb`, `unlink`  
  (see #376)
- Add `async_op` argument to `flushall` and `flushdb` commands  
  (see #364, #370)

### Bugfixes

- **Important!** Fix Sentinel sentinel client with pool `minsize`
  greater than 1  
  (see #380)
- Fix `SentinelPool.discover_timeout` usage  
  (see #379)
- Fix `Receiver` hang on disconnect  
  (see #354, #366)
- Fix an issue with `subscribe`/`psubscribe` with empty pool  
  (see #351, #355)
- Fix an issue when `StreamReader`'s feed_data is called before set_parser  
  (see #347)

### Miscellaneous

- Update dependencies versions
- Multiple test fixes

## 1.0.0 (2017-11-17)


### Features

- **Important!** Drop Python 3.3, 3.4 support  
  (see #321, #323, #326)

- **Important!** Connections pool has been refactored; now `create_redis`
  function will yield `Redis` instance instead of `RedisPool`  
  (see #129)
- **Important!** Change sorted set commands reply format:
  return list of tuples instead of plain list for commands
  accepting `withscores` argument  
  (see #334)
- **Important!** Change `hscan` command reply format:
  return list of tuples instead of mixed key-value list  
  (see #335)
- Implement Redis URI support as supported `address` argument value  
  (see #322)
- Dropped `create_reconnecting_redis`, `create_redis_pool` should be
  used instead
- Implement custom `StreamReader`  
  (see #273)
- Implement Sentinel support  
  (see #181)
- Implement pure-python parser  
  (see #212)
- Add `migrate_keys` command  
  (see #187)
- Add `zrevrangebylex` command  
  (see #201)
- Add `command`, `command_count`, `command_getkeys` and
  `command_info` commands  
  (see #229)
- Add `ping` support in pubsub connection  
  (see #264)
- Add `exist` parameter to `zadd` command  
  (see #288)
- Add `MaxClientsError` and implement `ReplyError` specialization  
  (see #325)
- Add `encoding` parameter to sorted set commands  
  (see #289)

### Bugfixes

- Fix `CancelledError` in `conn._reader_task`  
  (see #301)
- Fix pending commands cancellation with `CancelledError`,
  use explicit exception instead of calling `cancel()` method  
  (see #316)
- Correct error message on Sentinel discovery of master/slave with password  
  (see #327)
- Fix `bytearray` support as command argument  
  (see #329)
- Fix critical bug in patched asyncio.Lock  
  (see #256)
- Fix Multi/Exec transaction canceled error  
  (see #225)
- Add missing arguments to `create_redis` and `create_redis_pool`
- Fix deprecation warning  
  (see #191)
- Make correct `__aiter__()`  
  (see #192)
- Backward compatibility fix for `with (yield from pool) as conn:`  
  (see #205)
- Fixed pubsub receiver stop()  
  (see #211)

### Miscellaneous

- Multiple test fixes
- Add PyPy3 to build matrix
- Update dependencies versions
- Add missing Python 3.6 classifier

## 0.3.5 (2017-11-08)


### Bugfixes

- Fix for indistinguishable futures cancellation with `asyncio.CancelledError`  
  (see #316, cherry-picked from master)

## 0.3.4 (2017-10-25)


### Bugfixes

- Fix time command result decoding when using connection-wide encoding setting  
  (see #266)

## 0.3.3 (2017-06-30)


### Bugfixes

- Critical bug fixed in patched asyncio.Lock  
  (see #256)

## 0.3.2 (2017-06-21)


### Features

- Added `zrevrangebylex` command  
  (see #201 cherry-picked from master)
- Add connection timeout  
  (see #221, cherry-picked from master)

### Bugfixes

- Fixed pool close warning  
  (see #239, #236,
  cherry-picked from master
- Fixed asyncio Lock deadlock issue  
  (see #231, #241)

## 0.3.1 (2017-05-09)


### Bugfixes

- Fix pubsub Receiver missing iter() method  
  (see #203)

## 0.3.0 (2017-01-11)


### Features

- Pub/Sub connection commands accept `Channel` instances  
  (see #168)
- Implement new Pub/Sub MPSC (multi-producers, single-consumer) Queue --
  `aioredis.pubsub.Receiver`  
  (see #176)
- Add `aioredis.abc` module providing abstract base classes
  defining interface for basic lib components  (see #176)
- Implement Geo commands support  
  (see #177, #179)

### Bugfixes

- Minor tests fixes
### Miscellaneous

- Update examples and docs to use `async`/`await` syntax
  also keeping `yield from` examples for history  
  (see #173)
- Reflow Travis CI configuration; add Python 3.6 section  
  (see #170)
- Add AppVeyor integration to run tests on Windows  
  (see #180)
- Update multiple development requirements

## 0.2.9 (2016-10-24)


### Features

- Allow multiple keys in `EXISTS` command  
  (see #156, #157)

### Bugfixes

- Close RedisPool when connection to Redis failed  
  (see #136)
- Add simple `INFO` command argument validation  
  (see #140)
- Remove invalid uses of `next()`

### Miscellaneous

- Update devel.rst docs; update Pub/Sub Channel docs (cross-refs)
- Update MANIFEST.in to include docs, examples and tests in source bundle

## 0.2.8 (2016-07-22)


### Features

- Add `hmset_dict` command  
  (see #130)
- Add `RedisConnection.address` property
- RedisPool `minsize`/`maxsize` must not be `None`
- Implement `close()`/`wait_closed()`/`closed` interface for pool  
  (see #128)

### Bugfixes

- Add test for `hstrlen`
- Test fixes

### Miscellaneous

- Enable Redis 3.2.0 on Travis
- Add spell checking when building docs  
  (see #132)
- Documentation updated

## 0.2.7 (2016-05-27)


- `create_pool()` minsize default value changed to 1
- Fixed cancellation of wait_closed  
  (see #118)
- Fixed `time()` conversion to float  
  (see #126)
- Fixed `hmset()` method to return bool instead of `b'OK'`  
  (see [#12))
- Fixed multi/exec + watch issue (changed watch variable was causing
  `tr.execute()` to fail)  
  (see #121)
- Replace `asyncio.Future` uses with utility method  
  (get ready to Python 3.5.2 `loop.create_future()`)
- Tests switched from unittest to pytest (see [#12))
- Documentation updates

## 0.2.6 (2016-03-30)


- Fixed Multi/Exec transactions cancellation issue  
  (see #110, #114)
- Fixed Pub/Sub subscribe concurrency issue  
  (see #113, #115)
- Add SSL/TLS support  
  (see  #116)
- `aioredis.ConnectionClosedError` raised in `execute_pubsub` as well  
  (see #108)
- `Redis.slaveof()` method signature changed: now to disable
  replication one should call `redis.slaveof(None)` instead of `redis.slaveof()`
- More tests added

## 0.2.5 (2016-03-02)


- Close all Pub/Sub channels on connection close  
  (see #88)
- Add `iter()` method to `aioredis.Channel` allowing to use it
  with `async for`  
  (see #89)
- Inline code samples in docs made runnable and downloadable  
  (see #92)
- Python 3.5 examples converted to use `async`/`await` syntax  
  (see #93)
- Fix Multi/Exec to honor encoding parameter  
  (see #94, #97)
- Add debug message in `create_connection`  
  (see #90)
- Replace `asyncio.async` calls with wrapper that respects asyncio version  
  (see #101)
- Use NODELAY option for TCP sockets  
  (see #105)
- New `aioredis.ConnectionClosedError` exception added. Raised if
  connection to Redis server is lost  
  (see #108, #109)
- Fix RedisPool to close and drop connection in subscribe mode on release
- Fix `aioredis.util.decode` to recursively decode list responses
- More examples added and docs updated
- Add google groups link to README
- Bump year in LICENSE and docs

## 0.2.4 (2015-10-13)


- Python 3.5 `async` support:

  - New scan commands API (`iscan`, `izscan`, `ihscan`)
  - Pool made awaitable (allowing `with await pool: ...` and `async
    with pool.get() as conn:` constructs)
- Fixed dropping closed connections from free pool  
  (see #83)
- Docs updated

## 0.2.3 (2015-08-14)


- Redis cluster support work in progress
- Fixed pool issue causing pool growth over max size & `acquire` call hangs  
  (see #71)
- `info` server command result parsing implemented
- Fixed behavior of util functions  
  (see #70)
- `hstrlen` command added
- Few fixes in examples
- Few fixes in documentation

## 0.2.2 (2015-07-07)


- Decoding data with `encoding` parameter now takes into account
  list (array) replies  
  (see #68)
- `encoding` parameter added to following commands:

  - generic commands: keys, randomkey
  - hash commands: hgetall, hkeys, hmget, hvals
  - list commands: blpop, brpop, brpoplpush, lindex, lpop, lrange, rpop, rpoplpush
  - set commands: smembers, spop, srandmember
  - string commands: getrange, getset, mget
- Backward incompatibility:

  `ltrim` command now returns bool value instead of 'OK'
- Tests updated

## 0.2.1 (2015-07-06)


- Logging added (aioredis.log module)
- Fixed issue with `wait_message` in pub/sub  
  (see #66)

## 0.2.0 (2015-06-04)


- Pub/Sub support added
- Fix in `zrevrangebyscore` command  
  (see #62)
- Fixes/tests/docs

## 0.1.5 (2014-12-09)


- AutoConnector added
- wait_closed method added for clean connections shutdown
- `zscore` command fixed
- Test fixes

## 0.1.4 (2014-09-22)


- Dropped following Redis methods -- `Redis.multi()`,
  `Redis.exec()`, `Redis.discard()`
- `Redis.multi_exec` hack'ish property removed
- `Redis.multi_exec()` method added
- High-level commands implemented:

  - generic commands (tests)
  - transactions commands (api stabilization).

- Backward incompatibilities:

  - Following sorted set commands' API changed:

    `zcount`, `zrangebyscore`, `zremrangebyscore`, `zrevrangebyscore`
  - set string command' API changed


## 0.1.3 (2014-08-08)


- RedisConnection.execute refactored to support commands pipelining  
  (see #33)
- Several fixes
- WIP on transactions and commands interface
- High-level commands implemented and tested:

  - hash commands
  - hyperloglog commands
  - set commands
  - scripting commands
  - string commands
  - list commands

## 0.1.2 (2014-07-31)


- `create_connection`, `create_pool`, `create_redis` functions updated: db and password
  arguments made keyword-only  
  (see #26)
- Fixed transaction handling  
  (see #32)
- Response decoding  
  (see #16)

## 0.1.1 (2014-07-07)


- Transactions support (in connection, high-level commands have some issues)
- Docs & tests updated.


## 0.1.0 (2014-06-24)


- Initial release
- RedisConnection implemented
- RedisPool implemented
- Docs for RedisConnection & RedisPool
- WIP on high-level API.
