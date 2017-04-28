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

    subscribers = await pub.publish_json('chan:1', {'Hello': 'World'})
    assert subscribers > 1
    subscribers = await pub.publish_json('chan:2', ['message'])
    assert subscribers > 1
    loop.call_later(0, mpsc.stop)
    # await asyncio.sleep(0, loop=loop)
    assert await tsk == [
        (snd1, b'{"Hello": "World"}'),
        (snd3, (b'chan:1', b'{"Hello": "World"}')),
        (snd2, b'["message"]'),
        (snd3, (b'chan:2', b'["message"]')),
        ]
    assert not mpsc.is_active


@pytest.mark.run_loop(timeout=5)
async def test_pubsub_receiver_call_stop_with_empty_queue(
        create_redis, server, loop):
    sub = await create_redis(server.tcp_address, loop=loop)

    mpsc = Receiver(loop=loop)

    # FIXME: currently at least one subscriber is needed
    snd1, = await sub.subscribe(mpsc.channel('chan:1'))

    now = loop.time()
    loop.call_later(.5, mpsc.stop)
    async for i in mpsc.iter():  # noqa (flake8 bug with async for)
        assert False, "StopAsyncIteration not raised"
    dt = loop.time() - now
    assert dt <= 1.5
    assert not mpsc.is_active
