import asyncio

import aioredis


async def main():
    # Create a redis client bound to a connection pool.
    redis = aioredis.from_url(
        "redis://localhost", encoding="utf-8", decode_responses=True
    )
    # get a redis client bound to a single connection.
    async with redis.client() as conn:
        ok = await conn.execute_command("set", "my-key", "some value")
        assert ok is True

        str_value = await conn.execute_command("get", "my-key")
        assert str_value == "some value"

        print("str value:", str_value)
    # The connection is automatically release to the pool


async def main_single():
    # Create a redis client with only a single connection.
    redis = aioredis.Redis(
        host="localhost",
        encoding="utf-8",
        decode_responses=True,
        single_connection_client=True,
    )
    ok = await redis.execute_command("set", "my-key", "some value")
    assert ok is True

    str_value = await redis.execute_command("get", "my-key")
    assert str_value == "some value"

    print("str value:", str_value)
    # the connection is automatically closed by GC.


if __name__ == "__main__":
    asyncio.run(main())
    asyncio.run(main_single())
