import asyncio

import aioredis.sentinel


async def main():
    sentinel = aioredis.sentinel.Sentinel([("localhost", 26379), ("sentinel2", 26379)])
    redis = sentinel.master_for("mymaster")

    ok = await redis.set("key", "value")
    assert ok
    val = await redis.get("key")
    assert val == b"value"


if __name__ == "__main__":
    asyncio.run(main())
