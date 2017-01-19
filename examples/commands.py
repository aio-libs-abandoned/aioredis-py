import asyncio
import aioredis


async def main():
    # Redis client bound to single connection (no auto reconnection).
    redis = await aioredis.create_redis(
        ('localhost', 6379))
    await redis.set('my-key', 'value')
    val = await redis.get('my-key')
    print(val)

    # gracefully closing underlying connection
    redis.close()
    await redis.wait_closed()


async def redis_pool():
    # Redis client bound to pool of connections (auto-reconnecting).
    redis = await aioredis.create_redis_pool(
        ('localhost', 6379))
    await redis.set('my-key', 'value')
    val = await redis.get('my-key')
    print(val)

    # gracefully closing underlying connection
    redis.close()
    await redis.wait_closed()


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
    asyncio.get_event_loop().run_until_complete(redis_pool())
