import asyncio
import aioredis


async def main():
    redis = await aioredis.create_redis_pool('redis://localhost')

    tr = redis.multi_exec()
    tr.set('key1', 'value1')
    tr.set('key2', 'value2')
    ok1, ok2 = await tr.execute()
    assert ok1
    assert ok2

asyncio.run(main())
