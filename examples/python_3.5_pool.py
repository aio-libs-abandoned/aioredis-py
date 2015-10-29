import asyncio
import aioredis


@asyncio.coroutine
def main():

    pool = yield from aioredis.create_pool(
        ('localhost', 6379))

    with (yield from pool) as conn:
        yield from conn.set('my-key', 'value')

    yield from async_with(pool)
    yield from with_await(pool)
    yield from pool.clear()


@asyncio.coroutine
async def async_with(pool):
    async with pool.get() as conn:
        value = await conn.get('my-key')
        print('raw value:', value)


@asyncio.coroutine
async def with_await(pool):
    with (await pool) as conn:
        value = await conn.get('my-key')
        print('raw value:', value)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
