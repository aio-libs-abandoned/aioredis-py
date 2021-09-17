# Migrating to v2.0

##  TL;DR

aioredis v2.0 is now a completely compliant asyncio-native implementation of redis-py.
The entire core and public API has been re-written to follow redis-py's implementation
as closely as possible. This means there are some major changes to the connection
interface, but we now have an interface that is completely consistent across the two
libraries, so porting from redis-py to aioredis should (in theory) be as easy as
changing some imports and adding `await` statements where needed.

## Motivations

As of December, 2019, this library had been abandoned. The code had grown stale, issues
were piling up, and we were rapidly becoming out-of-date with Redis's own feature-set.
In light of this, the decision was made to ensure that the barrier to support for
potential maintainers or contributors would be minimized. A running port of redis-py
accomplishes this.

Additionally, embracing redis-py as the de-facto implementation lowers the barrier to
entry for new adopters, as the interface is now nearly identical.

## Connecting to Redis

Connecting to redis is no longer done by calling `await aioredis.create_pool(...)`.
Instead, the default mode of initialization is the same as redis-py:

```python
--8<-- "examples/commands.py"
```

As you can see, the `from_url(...)` methods are not coroutine functions. This is because
connections are created lazily rather than pre-filled, in keeping with the pattern
established in redis-py. If you have a need to establish a connection immediately, you
can use `aioredis.Redis.ping` to fetch a connection and ping your redis instance.

## Pipelines and Transactions (Multi/Exec)


In the previous implementation, Pipeline was essentially an abstraction over
`asyncio.gather`. Every command set to the Pipeline was immediately pushed to Redis in
its own task.

This had a few surprising side-effects:


1. aioredis Pipelines where much slower that redis-py Pipelines.
2. An exception in one command didn't necessarily halt execution of the Pipeline.
3. We weren't taking full advantage of Redis's Multi/Bulk protocol.
4. We could potentially overload Redis with too many individual commands at once.

The `aioredis.Pipeline` is now fully compliant with the original redis-py
implementation:

```python
--8<-- "examples/getting_started/03_multiexec.py"
```

```python
--8<-- "examples/transaction.py"
```

## Cleaning Up

You may have noticed in the examples above that we don't explicitly close our
connections when the main function exits. This is because, again like redis-py, our
clients and connections have implemented `__del__` methods, so will attempt to
automatically clean up any open connections when garbage-collected.

If, however, you find you need to manage your connections lifecycle yourself, you can
do so like this:


```python
import asyncio
import aioredis


async def main():
    pool = aioredis.ConnectionPool.from_url(
        "redis://localhost", decode_responses=True
    )
    meanings = aioredis.Redis(connection_pool=pool)
    try:
        await meanings.set("life", 42)
        print(f"The answer: {await meanings.get('life')}")
    finally:
        await pool.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
```


## What's New

aioredis is now *fully compliant* with Redis 6.0 including:

1. ACL/RBAC
2. Full PubSub support (2.8.0)

Some of these features were partially implemented or implemented with many known issues,
but are now considered stable.


## What's Gone

We no longer ship with a `Channel` abstraction over PubSub. This implementation was
buggy and not well understood. Additionally, there is no such abstraction in our source
implementation (redis-py).


## Next Steps

You're encouraged to review our Getting Started page, API docs, and Examples page to get
up-to-date on the latest aioredis implementation.

--8<-- "includes/glossary.md"
