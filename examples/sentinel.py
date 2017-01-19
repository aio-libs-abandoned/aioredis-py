import asyncio
import aioredis


async def main():
    sentinel_client = await aioredis.create_sentinel(
        [('localhost', 26379)])

    master_redis = sentinel_client.master_for('mymaster')
    info = await master_redis.role()
    print("Master role:", info)
    assert info.role == 'master'

    sentinel_client.close()
    await sentinel_client.wait_closed()


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
