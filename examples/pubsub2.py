import asyncio
import aioredis


async def pubsub():
    sub = await aioredis.create_redis(
         ('localhost', 6379))

    ch1, ch2 = await sub.subscribe('channel:1', 'channel:2')
    assert isinstance(ch1, aioredis.Channel)
    assert isinstance(ch2, aioredis.Channel)

    async def async_reader(channel):
        while await channel.wait_message():
            msg = await channel.get(encoding='utf-8')
            # ... process message ...
            print("message in {}: {}".format(channel.name, msg))

    tsk1 = asyncio.ensure_future(async_reader(ch1))

    # Or alternatively:

    async def async_reader2(channel):
        while True:
            msg = await channel.get(encoding='utf-8')
            if msg is None:
                break
            # ... process message ...
            print("message in {}: {}".format(channel.name, msg))

    tsk2 = asyncio.ensure_future(async_reader2(ch2))

    # Publish messages and terminate
    pub = await aioredis.create_redis(
        ('localhost', 6379))
    while True:
        channels = await pub.pubsub_channels('channel:*')
        if len(channels) == 2:
            break

    for msg in ("Hello", ",", "world!"):
        for ch in ('channel:1', 'channel:2'):
            await pub.publish(ch, msg)
    pub.close()
    sub.close()
    await asyncio.sleep(0)
    await pub.wait_closed()
    await sub.wait_closed()
    await asyncio.gather(tsk1, tsk2)


if __name__ == '__main__':
    import os
    if 'redis_version:2.6' not in os.environ.get('REDIS_VERSION', ''):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(pubsub())
