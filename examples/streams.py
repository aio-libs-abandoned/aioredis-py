import asyncio
from collections import OrderedDict

import aioredis

"""
docker run --rm -it -p 6379:6379 redis:5.0-rc5-alpine

you can use docker exec inside the redis container
use the redis-cli to create messages and groups.

then from another terminal
AIOREDIS_DEBUG=1 python experiment.py
"""

STREAM = "my-stream"


async def add_messages(number=10):
    redis = await aioredis.create_redis("redis://localhost")

    for i in range(number):

        fields = OrderedDict(((b"message", number),))
        await redis.xadd(STREAM, fields)
        print(f"Message n.{i} published")

        await asyncio.sleep(2)


async def streams_low_level():
    redis = await aioredis.create_redis("redis://localhost")

    messages = await redis.xread([STREAM])

    while True:
        messages = await redis.xread([STREAM])
        for message in messages:
            print(message)

        await asyncio.sleep(2)


async def streams_high_level():
    redis = await aioredis.create_redis("redis://localhost")
    streams = redis.streams.consumer([STREAM], encoding="utf-8")

    while True:
        msg = await streams.get()
        print(msg)

        await asyncio.sleep(2)


async def streams_with_group():
    redis = await aioredis.create_redis("redis://localhost")
    streams = redis.streams.consumer_with_group(
        [STREAM], group_name="mygroup", consumer_name="Alice", encoding="utf-8"
    )

    while True:
        message = await streams.get()
        print(message)
        await streams.ack_message(message[1])

        await asyncio.sleep(2)


async def stream_async_for():
    redis = await aioredis.create_redis("redis://localhost")
    streams = redis.streams.consumer([STREAM], encoding="utf-8")

    async for message in streams:
        print(message)

        await asyncio.sleep(2)


async def stream_async_for_group():
    redis = await aioredis.create_redis("redis://localhost")
    streams = redis.streams.consumer_with_group(
        [STREAM], group_name="mygroup", consumer_name="Alice", encoding="utf-8"
    )

    async for message in streams:
        print(message)
        await asyncio.sleep(5)
        await streams.ack_message(message[1])
        print(streams.last_ids_for_stream)
        print(streams)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    # loop.create_task(add_messages())
    loop.create_task(streams_high_level())
    loop.run_forever()
