import asyncio
import aioredis


@asyncio.coroutine
def main():
    redis = yield from aioredis.create_redis(
        ('localhost', 6379))

    @asyncio.coroutine
    def transaction():
        tr = redis.multi_exec()
        future1 = tr.set('foo', '123')
        future2 = tr.set('bar', '321')
        result = yield from tr.execute()
        assert result == (yield from asyncio.gather(future1, future2))
        return result

    redis.close()
    yield from redis.wait_closed()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
