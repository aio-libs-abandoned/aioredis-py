import asyncio

import aioredis


async def main():
    redis = aioredis.from_url("redis://localhost", max_connections=10)
    await redis.execute_command("set", "my-key", "value")
    val = await redis.execute_command("get", "my-key")
    print("raw value:", val)


async def main_pool():
    pool = aioredis.ConnectionPool.from_url("redis://localhost", max_connections=10)
    redis = aioredis.Redis(connection_pool=pool)
    await redis.execute_command("set", "my-key", "value")
    val = await redis.execute_command("get", "my-key")
    print("raw value:", val)


if __name__ == "__main__":
    asyncio.run(main())
    asyncio.run(main_pool())
