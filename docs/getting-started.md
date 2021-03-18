# Getting Started

## Installation

```shell
pip install aioredis
```

This will install `aioredis`, `async-timeout`.

### With hiredis

```shell
pip install aioredis[hiredis]
```

### Installing from Git

```shell
pip install git+https://github.com/aio-libs/aioredis@master#egg=aioredis
```


## Connecting

```python
--8<-- "examples/getting_started/00_connect.py"
```

`aioredis.from_url` creates a Redis client backed by a pool of connections. The only
required argument is the URL, which should be string representing a TCP or UNIX socket
address.

See the [high-level](api/high-level.md) API reference for a full list of supported
commands.

### Connecting to a Specific Database

There are two ways to specify a database index to set your connection pool to:

1. Pass the index in as a keyward argument when initializing the client
   ```python
   import aioredis

   redis = await aioredis.from_url("redis://localhost",  db=1)
   ```

2. Pass the index as a path component in the URI
   ```python
   import aioredis

   redis = await aioredis.from_url("redis://localhost/1")
   ```

!!! note

    DB index specified in URI will take precedence over
    ``db`` keyword argument.

### Connecting to an ACL-Protected Redis Instance

Similarly, the username/password can be specified via a keyword argument or via the URI.
The values in the URI will always take precedence.

1. Via keyword-arguments:
   ```python
   import aioredis

   redis = await aioredis.from_url(
       "redis://localhost", username="user", password="sEcRet"
   )
   ```

2. Via the AUTH section of the URI:
   ```python
   import aioredis

   redis = await aioredis.from_url("redis://user:sEcRet@localhost/")
   ```


## Response Decoding
By default `aioredis` will return `bytes` for most Redis commands that return string
replies. Redis error replies are known to be valid UTF-8 strings so error messages are
decoded automatically.

If you know that data in Redis is valid string you can tell `aioredis` to decode result
by passing `decode_responses=True` in a command call:

```python
--8<-- "examples/getting_started/01_decoding.py"
```

By default, `aioredis` will automatically decode lists, hashes, sets, etc:

```python
--8<-- "examples/getting_started/02_decoding.py"
```

## Transactions (Multi/Exec)

```python
--8<-- "examples/getting_started/03_multiexec.py"
```

The `aioredis.Redis.pipeline` will return a `aioredis.Pipeline` object, which will
buffer all commands in-memory and compile them into batches using the
[Redis Bulk String](https://redis.io/topics/protocol) protocol. Additionally, each
command will return the Pipeline instance, allowing you to chain your commands,
i.e., `p.set('foo', 1).set('bar', 2).mget('foo', 'bar')`.

The commands will not be reflected in Redis until `execute()` is called & awaited.

Usually, when performing a bulk operation, taking advantage of a "transaction" (e.g.,
Multi/Exec) is to be desired, as it will also add a layer of atomicity to your bulk
operation.


## Pub/Sub Mode

`aioredis` provides support for Redis Publish/Subscribe messaging.

Subscribing to specific channels:

```python
--8<-- "examples/getting_started/04_pubsub.py"
```

Subscribing to channels matching a glob-style pattern:

```python
--8<-- "examples/getting_started/05_pubsub.py"
```

## Redis Sentinel Client

```python
--8<-- "examples/getting_started/06_sentinel.py"
```

The Sentinel client requires a list of Redis Sentinel addresses to connect to and start
discovering services.

Calling `aioredis.sentinel.Sentinel.master_for` or
`aioredis.sentinel.Sentinel.slave_for` methods will return Redis clients connected to
specified services monitored by Sentinel.

Sentinel client will detect failover and reconnect Redis clients automatically.


--8<-- "includes/glossary.md"
