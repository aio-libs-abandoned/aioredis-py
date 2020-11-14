import asyncio
import aioredis


async def main():
    pool = await aioredis.create_pool("redis://localhost", minsize=5, maxsize=10)
    with await pool as conn:  # low-level redis connection
        await conn.execute("set", "my-key", "value")
        val = await conn.execute("get", "my-key")
    print("raw value:", val)
    pool.close()
    await pool.wait_closed()  # closing all open connections


if __name__ == "__main__":
    asyncio.run(main())
