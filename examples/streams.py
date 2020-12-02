import asyncio

import aioredis

"""
docker run --rm -it -p 6379:6379 redis:5.0-rc5-alpine

you can use docker exec inside the redis container
use the redis-cli to create messages and groups.

then from another terminal
AIOREDIS_DEBUG=1 python experiment.py
"""


async def stream_low():
    redis = await aioredis.create_redis("redis://localhost")

    while True:
        messages = await self._redis.xread(
            streams=streams, count=self._count, latest_ids=latest_ids
        )
        for message in messages:
            print(msg)

        await asyncio.sleep(5)


async def stream():
    redis = await aioredis.create_redis("redis://localhost")
    streams = redis.streams.consumer(["mystream"], encoding="utf-8")

    while True:
        msg = await streams.get()
        print(msg)

        await asyncio.sleep(5)


async def with_group():
    redis = await aioredis.create_redis("redis://localhost")
    streams = redis.streams.consumer_with_group(
        ["mystream"], group_name="mygroup", consumer_name="Alice", encoding="utf-8"
    )

    while True:
        msg = await streams.get()
        print(msg)
        await streams.ack_message(msg[1])
        await asyncio.sleep(5)


async def another_task():
    while True:
        print("Tired")
        await asyncio.sleep(2)
        print("Rested")


async def stream_async_for():
    redis = await aioredis.create_redis("redis://localhost")
    streams = redis.streams.consumer(["mystream"], encoding="utf-8")

    async for message in streams:
        print(message)
        await asyncio.sleep(5)


async def stream_async_for_group():
    redis = await aioredis.create_redis("redis://localhost")
    streams = redis.streams.consumer_with_group(
        ["mystream"], group_name="mygroup", consumer_name="Alice", encoding="utf-8"
    )

    async for message in streams:
        print(message)
        await asyncio.sleep(5)
        await streams.ack_message(message[1])
        print(streams.last_ids_for_stream)
        print(streams)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(stream_async_for_group())
    # loop.create_task(another_task())
    loop.run_forever()
