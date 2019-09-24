import asyncio
import aioredis


async def main():
    redis = await aioredis.create_redis_pool('redis://localhost')

    ch, = await redis.psubscribe('channel:*')
    assert isinstance(ch, aioredis.Channel)

    async def reader(channel):
        async for ch, message in channel.iter():
            print("Got message in channel:", ch, ":", message)
    asyncio.get_running_loop().create_task(reader(ch))

    await redis.publish('channel:1', 'Hello')
    await redis.publish('channel:2', 'World')

    redis.close()
    await redis.wait_closed()

asyncio.run(main())
