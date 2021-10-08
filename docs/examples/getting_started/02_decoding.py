import asyncio

import aioredis


async def main():
    redis = aioredis.from_url("redis://localhost", decode_responses=True)

    await redis.hset("hash", mapping={"key1":"value1", "key2":"value2", "key3":123})

    result = await redis.hgetall("hash")
    assert result == {
        "key1": "value1",
        "key2": "value2",
        "key3": "123",  # note that Redis returns int as string
    }

    await redis.close()


asyncio.run(main())
