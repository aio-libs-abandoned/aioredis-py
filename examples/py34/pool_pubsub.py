import asyncio
import aioredis


STOPWORD = 'STOP'


@asyncio.coroutine
def pubsub():
    pool = yield from aioredis.create_pool(
        ('localhost', 6379),
        minsize=5, maxsize=10)

    @asyncio.coroutine
    def reader(channel):
        while (yield from channel.wait_message()):
            msg = yield from channel.get(encoding='utf-8')
            # ... process message ...
            print("message in {}: {}".format(channel.name, msg))

            if msg == STOPWORD:
                return

    with (yield from pool) as redis:
        channel, = yield from redis.subscribe('channel:1')
        yield from reader(channel)  # wait for reader to complete
        yield from redis.unsubscribe('channel:1')

    # Explicit redis usage
    redis = yield from pool.acquire()
    try:
        channel, = yield from redis.subscribe('channel:1')
        yield from reader(channel)  # wait for reader to complete
        yield from redis.unsubscribe('channel:1')
    finally:
        pool.release(redis)

    pool.close()
    yield from pool.wait_closed()    # closing all open connections


def main():
    loop = asyncio.get_event_loop()
    tsk = asyncio.async(pubsub(), loop=loop)

    @asyncio.coroutine
    def publish():
        pub = yield from aioredis.create_redis(
            ('localhost', 6379))
        while not tsk.done():
            # wait for clients to subscribe
            while True:
                subs = yield from pub.pubsub_numsub('channel:1')
                if subs[b'channel:1'] == 1:
                    break
                yield from asyncio.sleep(0, loop=loop)
            # publish some messages
            for msg in ['one', 'two', 'three']:
                yield from pub.publish('channel:1', msg)
            # send stop word
            yield from pub.publish('channel:1', STOPWORD)
        pub.close()
        yield from pub.wait_closed()

    loop.run_until_complete(asyncio.gather(publish(), tsk, loop=loop))


if __name__ == '__main__':
    import os
    if 'redis_version:2.6' not in os.environ.get('REDIS_VERSION', ''):
        main()
