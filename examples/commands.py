import asyncio
import aioredis


def main():
    loop = asyncio.get_event_loop()

    async def go():
        redis = await aioredis.create_redis(
            ('localhost', 6379))
        await redis.set('my-key', 'value')
        val = await redis.get('my-key')
        print(val)

        # gracefully closing underlying connection
        redis.close()
        await redis.wait_closed()
    loop.run_until_complete(go())


if __name__ == '__main__':
    main()
