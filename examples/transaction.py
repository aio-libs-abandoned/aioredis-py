import asyncio
import aioredis


async def main():
    redis = await aioredis.create_redis(
        ('localhost', 6379))
    await redis.delete('foo', 'bar')
    tr = redis.multi_exec()
    fut1 = tr.incr('foo')
    fut2 = tr.incr('bar')
    res = await tr.execute()
    res2 = await asyncio.gather(fut1, fut2)
    print(res)
    assert res == res2

    redis.close()
    await redis.wait_closed()


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
