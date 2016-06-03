import asyncio
import pytest


@pytest.mark.run_loop
async def test_pubsub_channel_iter(create_redis, server, loop):
    sub = await create_redis(server.tcp_address, loop=loop)
    pub = await create_redis(server.tcp_address, loop=loop)

    ch, = await sub.subscribe('chan:1')

    async def coro(ch):
        lst = []
        async for msg in ch.iter():
            lst.append(msg)
        return lst

    tsk = asyncio.ensure_future(coro(ch), loop=loop)
    await pub.publish_json('chan:1', {'Hello': 'World'})
    await pub.publish_json('chan:1', ['message'])
    await asyncio.sleep(0, loop=loop)
    ch.close()
    assert await tsk == [b'{"Hello": "World"}', b'["message"]']
