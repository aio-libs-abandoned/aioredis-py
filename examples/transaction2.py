import asyncio
import aioredis


async def main():
    redis = await aioredis.create_redis(
        'redis://localhost')

    async def transaction():
        tr = redis.multi_exec()
        future1 = tr.set('foo', '123')
        future2 = tr.set('bar', '321')
        result = await tr.execute()
        assert result == await asyncio.gather(future1, future2)
        return result

    await transaction()
    redis.close()
    await redis.wait_closed()


if __name__ == '__main__':
    asyncio.run(main())
