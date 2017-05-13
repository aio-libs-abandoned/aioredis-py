import asyncio
import aioredis


@asyncio.coroutine
def pubsub():
    sub = yield from aioredis.create_redis(
         ('localhost', 6379))

    ch1, ch2 = yield from sub.subscribe('channel:1', 'channel:2')
    assert isinstance(ch1, aioredis.Channel)
    assert isinstance(ch2, aioredis.Channel)

    @asyncio.coroutine
    def async_reader(channel):
        while (yield from channel.wait_message()):
            msg = yield from channel.get(encoding='utf-8')
            # ... process message ...
            print("message in {}: {}".format(channel.name, msg))

    tsk1 = asyncio.async(async_reader(ch1))

    # Or alternatively:

    @asyncio.coroutine
    def async_reader2(channel):
        while True:
            msg = yield from channel.get(encoding='utf-8')
            if msg is None:
                break
            # ... process message ...
            print("message in {}: {}".format(channel.name, msg))

    tsk2 = asyncio.async(async_reader2(ch2))

    # Publish messages and terminate
    pub = yield from aioredis.create_redis(
        ('localhost', 6379))
    while True:
        channels = yield from pub.pubsub_channels('channel:*')
        if len(channels) == 2:
            break

    for msg in ("Hello", ",", "world!"):
        for ch in ('channel:1', 'channel:2'):
            yield from pub.publish(ch, msg)
    pub.close()
    sub.close()
    yield from asyncio.sleep(0)
    yield from pub.wait_closed()
    yield from sub.wait_closed()
    yield from asyncio.gather(tsk1, tsk2)


if __name__ == '__main__':
    import os
    if 'redis_version:2.6' not in os.environ.get('REDIS_VERSION', ''):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(pubsub())
