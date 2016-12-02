import asyncio
import aioredis


def main():
    loop = asyncio.get_event_loop()

    async def go():
        pool = await aioredis.create_pool(
            ('localhost', 6379),
            minsize=5, maxsize=10)
        with await pool as redis:    # high-level redis API instance
            await redis.set('my-key', 'value')
            val = await redis.get('my-key')
        print('raw value:', val)
        pool.close()
        await pool.wait_closed()    # closing all open connections

    loop.run_until_complete(go())


if __name__ == '__main__':
    main()
