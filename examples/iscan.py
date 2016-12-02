import asyncio
import aioredis


async def main():

    redis = await aioredis.create_redis(
        ('localhost', 6379))

    await redis.delete('something:hash',
                       'something:set',
                       'something:zset')
    await redis.mset('something', 'value',
                     'something:else', 'else')
    await redis.hmset('something:hash',
                      'something:1', 'value:1',
                      'something:2', 'value:2')
    await redis.sadd('something:set', 'something:1',
                     'something:2', 'something:else')
    await redis.zadd('something:zset', 1, 'something:1',
                     2, 'something:2', 3, 'something:else')

    await go(redis)
    redis.close()
    await redis.wait_closed()


async def go(redis):
    async for key in redis.iscan(match='something*'):
        print('Matched:', key)

    key = 'something:hash'

    async for name, val in redis.ihscan(key, match='something*'):
        print('Matched:', name, '->', val)

    key = 'something:set'

    async for val in redis.isscan(key, match='something*'):
        print('Matched:', val)

    key = 'something:zset'

    async for val, score in redis.izscan(key, match='something*'):
        print('Matched:', val, ':', score)


if __name__ == '__main__':
    import os
    if 'redis_version:2.6' not in os.environ.get('REDIS_VERSION', ''):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
