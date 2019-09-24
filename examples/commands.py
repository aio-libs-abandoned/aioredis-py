import asyncio
import aioredis


async def main():
    # Redis client bound to single connection (no auto reconnection).
    redis = await aioredis.create_redis(
        'redis://localhost')
    await redis.set('my-key', 'value')
    val = await redis.get('my-key')
    print(val)

    # gracefully closing underlying connection
    redis.close()
    await redis.wait_closed()


async def redis_pool():
    # Redis client bound to pool of connections (auto-reconnecting).
    redis = await aioredis.create_redis_pool(
        'redis://localhost')
    await redis.set('my-key', 'value')
    val = await redis.get('my-key')
    print(val)

    # gracefully closing underlying connection
    redis.close()
    await redis.wait_closed()


if __name__ == '__main__':
    asyncio.run(main())
    asyncio.run(redis_pool())
