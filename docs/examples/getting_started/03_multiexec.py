import asyncio

import aioredis


async def main():
    redis = await aioredis.from_url("redis://localhost")
    async with redis.pipeline(transaction=True) as pipe:
        ok1, ok2 = await (pipe.set("key1", "value1").set("key2", "value2").execute())
    assert ok1
    assert ok2


if __name__ == "__main__":
    asyncio.run(main())
