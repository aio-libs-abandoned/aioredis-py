import asyncio
import aioredis


async def main():
    redis = await aioredis.create_redis_pool('redis://localhost')

    ch1, ch2 = await redis.subscribe('channel:1', 'channel:2')
    assert isinstance(ch1, aioredis.Channel)
    assert isinstance(ch2, aioredis.Channel)

    async def reader(channel):
        async for message in channel.iter():
            print("Got message:", message)
    asyncio.get_running_loop().create_task(reader(ch1))
    asyncio.get_running_loop().create_task(reader(ch2))

    await redis.publish('channel:1', 'Hello')
    await redis.publish('channel:2', 'World')

    redis.close()
    await redis.wait_closed()

asyncio.run(main())
