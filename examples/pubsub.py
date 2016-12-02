import asyncio
import aioredis


def main():
    loop = asyncio.get_event_loop()

    async def reader(ch):
        while (await ch.wait_message()):
            msg = await ch.get_json()
            print("Got Message:", msg)

    async def go():
        pub = await aioredis.create_redis(
            ('localhost', 6379))
        sub = await aioredis.create_redis(
            ('localhost', 6379))
        res = await sub.subscribe('chan:1')
        ch1 = res[0]

        tsk = asyncio.ensure_future(reader(ch1))

        res = await pub.publish_json('chan:1', ["Hello", "world"])
        assert res == 1

        await sub.unsubscribe('chan:1')
        await tsk
        sub.close()
        pub.close()

    loop.run_until_complete(go())


if __name__ == '__main__':
    main()
