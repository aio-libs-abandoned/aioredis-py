import asyncio

import aioredis


async def main():
    redis = aioredis.from_url("redis://localhost")
    await redis.set("my-key", "value")
    value = await redis.get("my-key")
    print(value)


asyncio.run(main())
