import asyncio
import aioredis


async def main():
    redis = await aioredis.create_redis(
        ('localhost', 6379))

    # No pipelining;
    async def wait_each_command():
        val = await redis.get('foo')    # wait until `val` is available
        cnt = await redis.incr('bar')   # wait until `cnt` is available
        return val, cnt

    # Sending multiple commands and then gathering results
    async def pipelined():
        fut1 = redis.get('foo')      # issue command and return future
        fut2 = redis.incr('bar')     # issue command and return future
        # block until results are available
        val, cnt = await asyncio.gather(fut1, fut2)
        return val, cnt

    # Explicit pipeline
    async def explicit_pipeline():
        pipe = redis.pipeline()
        fut1 = pipe.get('foo')
        fut2 = pipe.incr('bar')
        result = await pipe.execute()
        val, cnt = await asyncio.gather(fut1, fut2)
        assert result == [val, cnt]
        return val, cnt

    res = await wait_each_command()
    print(res)
    res = await pipelined()
    print(res)
    res = await explicit_pipeline()
    print(res)

    redis.close()
    await redis.wait_closed()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
