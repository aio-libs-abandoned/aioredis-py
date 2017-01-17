import asyncio
import aioredis

# The code that is commented out is for Python 3.4 
# It is being left here for purposes of making it easy to reference Py3.4 vs. Py3.5
'''
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
 '''

def main():
    loop = asyncio.get_event_loop()

    async def go():
        redis = await aioredis.create_redis(
            ('localhost', 6379))
        await redis.set('my-key', 'value')
        val = await redis.get('my-key')
        print(val)

        # optinally closing underlying connection
        redis.close()
    loop.run_until_complete(go())


if __name__ == '__main__':
    main()
