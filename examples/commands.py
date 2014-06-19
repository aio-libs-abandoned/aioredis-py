import asyncio
import aioredis


def main():
    loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def go():
        redis = yield from aioredis.create_redis(
            ('localhost', 6379))
        yield from redis.set('my-key', 'value')
        val = yield from redis.get('my-key')
        print(val)

        # optinally closing underlying connection
        redis.close()
    loop.run_until_complete(go())


if __name__ == '__main__':
    main()
