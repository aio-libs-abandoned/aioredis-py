import asyncio
import aioredis


async def main():
    redis = await aioredis.create_redis('redis://localhost')
    streams = aioredis.ReadGroupStream(["mystream"], True, "group", "one", redis)
    await streams.create_group()

    while True:
        msg = await streams.get()
        # ... process message ...
        print(msg)
        await streams.ack_message(msg[1])
        await asyncio.sleep(2)

async def another_task():
    while True:
        print("Tired")
        await asyncio.sleep(2)
        print("Sleeped")


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(main())
    #loop.create_task(another_task())
    loop.run_forever()
