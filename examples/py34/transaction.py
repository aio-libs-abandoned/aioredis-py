import asyncio
import aioredis


def main():
    loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def go():
        redis = yield from aioredis.create_redis(
            ('localhost', 6379))
        yield from redis.delete('foo', 'bar')
        tr = redis.multi_exec()
        fut1 = tr.incr('foo')
        fut2 = tr.incr('bar')
        res = yield from tr.execute()
        res2 = yield from asyncio.gather(fut1, fut2)
        print(res)
        assert res == res2
        redis.close()
        yield from redis.wait_closed()

    loop.run_until_complete(go())


if __name__ == '__main__':
    main()
