import asyncio
import aioredis


def main():
    """Scan command example."""
    loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def go():
        redis = yield from aioredis.create_redis(
            ('localhost', 6379))

        yield from redis.mset('key:1', 'value1', 'key:2', 'value2')
        cur = b'0'  # set initial cursor to 0
        while cur:
            cur, keys = yield from redis.scan(cur, match='key:*')
            print("Iteration results:", keys)

        redis.close()
        yield from redis.wait_closed()
    loop.run_until_complete(go())


if __name__ == '__main__':
    import os
    if 'redis_version:2.6' not in os.environ.get('REDIS_VERSION', ''):
        main()
