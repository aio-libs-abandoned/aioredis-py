import asyncio
import pytest

from aioredis.pubsub import Receiver


@pytest.mark.run_loop
async def test_pubsub_receiver_iter(create_redis, server, loop):
    sub = await create_redis(server.tcp_address, loop=loop)
    pub = await create_redis(server.tcp_address, loop=loop)

    mpsc = Receiver(loop=loop)

    async def coro(mpsc):
        lst = []
        async for msg in mpsc.iter():
            lst.append(msg)
        return lst

    tsk = asyncio.ensure_future(coro(mpsc), loop=loop)
    snd1, = await sub.subscribe(mpsc.channel('chan:1'))
    snd2, = await sub.subscribe(mpsc.channel('chan:2'))
    snd3, = await sub.psubscribe(mpsc.pattern('chan:*'))

    await pub.publish_json('chan:1', {'Hello': 'World'})
    await pub.publish_json('chan:2', ['message'])
    mpsc.stop()
    await asyncio.sleep(0, loop=loop)
    assert await tsk == [
        (snd1, b'{"Hello": "World"}'),
        (snd3, (b'chan:1', b'{"Hello": "World"}')),
        (snd2, b'["message"]'),
        (snd3, (b'chan:2', b'["message"]')),
        ]
    assert not mpsc.is_active
