import asyncio
import aioredis


@asyncio.coroutine
def main():
    redis = yield from aioredis.create_redis(
        ('localhost', 6379))

    # No pipelining;
    @asyncio.coroutine
    def wait_each_command():
        val = yield from redis.get('foo')    # wait until `val` is available
        cnt = yield from redis.incr('bar')   # wait until `cnt` is available
        return val, cnt

    # Sending multiple commands and then gathering results
    @asyncio.coroutine
    def pipelined():
        fut1 = redis.get('foo')      # issue command and return future
        fut2 = redis.incr('bar')     # issue command and return future
        # block until results are available
        val, cnt = yield from asyncio.gather(fut1, fut2)
        return val, cnt

    # Convenient way
    @asyncio.coroutine
    def convenience_way():
        pipe = redis.pipeline()
        fut1 = pipe.get('foo')
        fut2 = pipe.incr('bar')
        result = yield from pipe.execute()
        val, cnt = yield from asyncio.gather(fut1, fut2)
        assert result == [val, cnt]
        return val, cnt

    res = yield from wait_each_command()
    print(res)
    res = yield from pipelined()
    print(res)
    res = yield from convenience_way()
    print(res)

    redis.close()
    yield from redis.wait_closed()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
