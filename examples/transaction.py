import asyncio
import aioredis


def main():
    loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def go():
        redis = yield from aioredis.create_redis(
            ('localhost', 6379))
        yield from redis.delete('foo', 'bar')
        res = yield from redis.multi_exec(
            redis.incr('foo'),
            redis.incr('bar'))
        print(res)

    loop.run_until_complete(go())

if __name__ == '__main__':
    main()
