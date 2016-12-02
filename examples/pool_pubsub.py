import asyncio
import aioredis


STOPWORD = 'STOP'


async def pubsub():
    pool = await aioredis.create_pool(
        ('localhost', 6379),
        minsize=5, maxsize=10)

    async def reader(channel):
        while (await channel.wait_message()):
            msg = await channel.get(encoding='utf-8')
            # ... process message ...
            print("message in {}: {}".format(channel.name, msg))

            if msg == STOPWORD:
                return

    with await pool as redis:
        channel, = await redis.subscribe('channel:1')
        await reader(channel)  # wait for reader to complete
        await redis.unsubscribe('channel:1')

    # Explicit redis usage
    redis = await pool.acquire()
    try:
        channel, = await redis.subscribe('channel:1')
        await reader(channel)  # wait for reader to complete
        await redis.unsubscribe('channel:1')
    finally:
        pool.release(redis)

    pool.close()
    await pool.wait_closed()    # closing all open connections


def main():
    loop = asyncio.get_event_loop()
    tsk = asyncio.async(pubsub(), loop=loop)

    async def publish():
        pub = await aioredis.create_redis(
            ('localhost', 6379))
        while not tsk.done():
            # wait for clients to subscribe
            while True:
                subs = await pub.pubsub_numsub('channel:1')
                if subs[b'channel:1'] == 1:
                    break
                await asyncio.sleep(0, loop=loop)
            # publish some messages
            for msg in ['one', 'two', 'three']:
                await pub.publish('channel:1', msg)
            # send stop word
            await pub.publish('channel:1', STOPWORD)
        pub.close()
        await pub.wait_closed()

    loop.run_until_complete(asyncio.gather(publish(), tsk, loop=loop))


if __name__ == '__main__':
    import os
    if 'redis_version:2.6' not in os.environ.get('REDIS_VERSION', ''):
        main()
