import asyncio

import aioredis


async def blocking_commands():
    # Redis client bound to pool of connections (auto-reconnecting).
    redis = await aioredis.create_redis_pool(
        'redis://localhost')

    async def get_message():
        # Redis blocking commands are NOT naturally async. They block the
        # connection that they are using. Therefore, if you exhaust a connection
        # pool, blocking commands could wind up blocking the event loop. To
        # avoid this unexpected behavior, always use explicit "acquire"
        # syntax with blocking commands.
        with await redis as r:
            return await r.brpop("my-key")

    future = asyncio.create_task(get_message())
    await redis.lpush('my-key', 'value')
    await future
    print(future.result())

    # gracefully closing underlying connection
    redis.close()
    await redis.wait_closed()


if __name__ == '__main__':
    asyncio.run(blocking_commands())
