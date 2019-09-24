import asyncio
import aioredis


async def main():
    sentinel = await aioredis.create_sentinel(
        ['redis://localhost:26379', 'redis://sentinel2:26379'])
    redis = sentinel.master_for('mymaster')

    ok = await redis.set('key', 'value')
    assert ok
    val = await redis.get('key', encoding='utf-8')
    assert val == 'value'

asyncio.run(main())
