import asyncio
import aioredis


def main():
    loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def reader(ch):
        while (yield from ch.wait_message()):
            msg = yield from ch.get_json()
            print("Got Message:", msg)

    @asyncio.coroutine
    def go():
        pub = yield from aioredis.create_redis(
            ('localhost', 6379))
        sub = yield from aioredis.create_redis(
            ('localhost', 6379))
        res = yield from sub.subscribe('chan:1')
        ch1 = res[0]

        tsk = asyncio.async(reader(ch1))

        res = yield from pub.publish_json('chan:1', ["Hello", "world"])
        assert res == 1

        yield from sub.unsubscribe('chan:1')
        yield from tsk
        sub.close()
        pub.close()

    loop.run_until_complete(go())


if __name__ == '__main__':
    main()
