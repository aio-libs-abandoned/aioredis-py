import pytest
import asyncio
import json
import sys

from unittest import mock

from aioredis import ChannelClosedError
from aioredis.abc import AbcChannel
from aioredis.pubsub import Receiver, _Sender
from _testutils import logs


def test_listener_channel(loop):
    mpsc = Receiver(loop=loop)
    assert not mpsc.is_active

    ch_a = mpsc.channel("channel:1")
    assert isinstance(ch_a, AbcChannel)
    assert mpsc.is_active

    ch_b = mpsc.channel('channel:1')
    assert ch_a is ch_b
    assert ch_a.name == ch_b.name
    assert ch_a.is_pattern == ch_b.is_pattern
    assert mpsc.is_active

    # remember id; drop refs to objects and create new one;
    ch_a.close()
    assert not ch_a.is_active

    assert not mpsc.is_active
    ch = mpsc.channel("channel:1")
    assert ch is not ch_a

    assert dict(mpsc.channels) == {b'channel:1': ch}
    assert dict(mpsc.patterns) == {}


def test_listener_pattern(loop):
    mpsc = Receiver(loop=loop)
    assert not mpsc.is_active

    ch_a = mpsc.pattern("*")
    assert isinstance(ch_a, AbcChannel)
    assert mpsc.is_active

    ch_b = mpsc.pattern('*')
    assert ch_a is ch_b
    assert ch_a.name == ch_b.name
    assert ch_a.is_pattern == ch_b.is_pattern
    assert mpsc.is_active

    # remember id; drop refs to objects and create new one;
    ch_a.close()
    assert not ch_a.is_active

    assert not mpsc.is_active
    ch = mpsc.pattern("*")
    assert ch is not ch_a

    assert dict(mpsc.channels) == {}
    assert dict(mpsc.patterns) == {b'*': ch}


@pytest.mark.run_loop
async def test_sender(loop):
    receiver = mock.Mock()

    sender = _Sender(receiver, 'name', is_pattern=False)
    assert isinstance(sender, AbcChannel)
    assert sender.name == b'name'
    assert sender.is_pattern is False
    assert sender.is_active is True

    with pytest.raises(RuntimeError):
        await sender.get()
    assert receiver.mock_calls == []

    sender.put_nowait(b'some data')
    assert receiver.mock_calls == [
        mock.call._put_nowait(b'some data', sender=sender),
        ]


def test_sender_close():
    receiver = mock.Mock()
    sender = _Sender(receiver, 'name', is_pattern=False)
    sender.close()
    assert receiver.mock_calls == [mock.call._close(sender, exc=None)]
    sender.close()
    assert receiver.mock_calls == [mock.call._close(sender, exc=None)]
    receiver.reset_mock()
    assert receiver.mock_calls == []
    sender.close()
    assert receiver.mock_calls == []


@pytest.mark.run_loop
async def test_subscriptions(create_connection, server, loop):
    sub = await create_connection(server.tcp_address, loop=loop)
    pub = await create_connection(server.tcp_address, loop=loop)

    mpsc = Receiver(loop=loop)
    await sub.execute_pubsub('subscribe',
                             mpsc.channel('channel:1'),
                             mpsc.channel('channel:3'))
    res = await pub.execute("publish", "channel:3", "Hello world")
    assert res == 1
    res = await pub.execute("publish", "channel:1", "Hello world")
    assert res == 1
    assert mpsc.is_active

    ch, msg = await mpsc.get()
    assert ch.name == b'channel:3'
    assert not ch.is_pattern
    assert msg == b"Hello world"

    ch, msg = await mpsc.get()
    assert ch.name == b'channel:1'
    assert not ch.is_pattern
    assert msg == b"Hello world"


@pytest.mark.run_loop
async def test_unsubscribe(create_connection, server, loop):
    sub = await create_connection(server.tcp_address, loop=loop)
    pub = await create_connection(server.tcp_address, loop=loop)

    mpsc = Receiver(loop=loop)
    await sub.execute_pubsub('subscribe',
                             mpsc.channel('channel:1'),
                             mpsc.channel('channel:3'))
    res = await pub.execute("publish", "channel:3", "Hello world")
    assert res == 1
    res = await pub.execute("publish", "channel:1", "Hello world")
    assert res == 1
    assert mpsc.is_active

    assert (await mpsc.wait_message()) is True
    ch, msg = await mpsc.get()
    assert ch.name == b'channel:3'
    assert not ch.is_pattern
    assert msg == b"Hello world"

    assert (await mpsc.wait_message()) is True
    ch, msg = await mpsc.get()
    assert ch.name == b'channel:1'
    assert not ch.is_pattern
    assert msg == b"Hello world"

    await sub.execute_pubsub('unsubscribe', 'channel:1')
    assert mpsc.is_active

    res = await pub.execute("publish", "channel:3", "message")
    assert res == 1
    assert (await mpsc.wait_message()) is True
    ch, msg = await mpsc.get()
    assert ch.name == b'channel:3'
    assert not ch.is_pattern
    assert msg == b"message"

    waiter = asyncio.ensure_future(mpsc.get(), loop=loop)
    await sub.execute_pubsub('unsubscribe', 'channel:3')
    assert not mpsc.is_active
    assert await waiter is None


@pytest.mark.run_loop
async def test_stopped(create_connection, server, loop):
    sub = await create_connection(server.tcp_address, loop=loop)
    pub = await create_connection(server.tcp_address, loop=loop)

    mpsc = Receiver(loop=loop)
    await sub.execute_pubsub('subscribe', mpsc.channel('channel:1'))
    assert mpsc.is_active
    mpsc.stop()

    with logs('aioredis', 'DEBUG') as cm:
        await pub.execute('publish', 'channel:1', b'Hello')
        await asyncio.sleep(0, loop=loop)

    assert len(cm.output) == 1
    # Receiver must have 1 EndOfStream message
    warn_messaege = (
        "WARNING:aioredis:Pub/Sub listener message after stop: "
        "sender: <_Sender name:b'channel:1', is_pattern:False, receiver:"
        "<Receiver is_active:False, senders:1, qsize:0>>, data: b'Hello'"
    )
    assert cm.output == [warn_messaege]

    # assert (await mpsc.get()) is None
    with pytest.raises(ChannelClosedError):
        await mpsc.get()
    res = await mpsc.wait_message()
    assert res is False


@pytest.mark.run_loop
async def test_wait_message(create_connection, server, loop):
    sub = await create_connection(server.tcp_address, loop=loop)
    pub = await create_connection(server.tcp_address, loop=loop)

    mpsc = Receiver(loop=loop)
    await sub.execute_pubsub('subscribe', mpsc.channel('channel:1'))
    fut = asyncio.ensure_future(mpsc.wait_message(), loop=loop)
    assert not fut.done()
    await asyncio.sleep(0, loop=loop)
    assert not fut.done()

    await pub.execute('publish', 'channel:1', 'hello')
    await asyncio.sleep(0, loop=loop)  # read in connection
    await asyncio.sleep(0, loop=loop)  # call Future.set_result
    assert fut.done()
    res = await fut
    assert res is True


@pytest.mark.run_loop
async def test_decode_message(loop):
    mpsc = Receiver(loop)
    ch = mpsc.channel('channel:1')
    ch.put_nowait(b'Some data')

    res = await mpsc.get(encoding='utf-8')
    assert isinstance(res[0], _Sender)
    assert res[1] == 'Some data'

    ch.put_nowait('{"hello": "world"}')
    res = await mpsc.get(decoder=json.loads)
    assert isinstance(res[0], _Sender)
    assert res[1] == {'hello': 'world'}

    ch.put_nowait(b'{"hello": "world"}')
    res = await mpsc.get(encoding='utf-8', decoder=json.loads)
    assert isinstance(res[0], _Sender)
    assert res[1] == {'hello': 'world'}


@pytest.mark.skipif(sys.version_info >= (3, 6),
                    reason="json.loads accept bytes since Python 3.6")
@pytest.mark.run_loop
async def test_decode_message_error(loop):
    mpsc = Receiver(loop)
    ch = mpsc.channel('channel:1')

    ch.put_nowait(b'{"hello": "world"}')
    unexpected = (mock.ANY, {'hello': 'world'})
    with pytest.raises(TypeError):
        assert (await mpsc.get(decoder=json.loads)) == unexpected

    ch = mpsc.pattern('*')
    ch.put_nowait((b'channel', b'{"hello": "world"}'))
    unexpected = (mock.ANY, b'channel', {'hello': 'world'})
    with pytest.raises(TypeError):
        assert (await mpsc.get(decoder=json.loads)) == unexpected


@pytest.mark.run_loop
async def test_decode_message_for_pattern(loop):
    mpsc = Receiver(loop)
    ch = mpsc.pattern('*')
    ch.put_nowait((b'channel', b'Some data'))

    res = await mpsc.get(encoding='utf-8')
    assert isinstance(res[0], _Sender)
    assert res[1] == (b'channel', 'Some data')

    ch.put_nowait((b'channel', '{"hello": "world"}'))
    res = await mpsc.get(decoder=json.loads)
    assert isinstance(res[0], _Sender)
    assert res[1] == (b'channel', {'hello': 'world'})

    ch.put_nowait((b'channel', b'{"hello": "world"}'))
    res = await mpsc.get(encoding='utf-8', decoder=json.loads)
    assert isinstance(res[0], _Sender)
    assert res[1] == (b'channel', {'hello': 'world'})


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


@pytest.mark.run_loop
async def test_pubsub_receiver_stop_on_disconnect(create_redis, server, loop):
    pub = await create_redis(server.tcp_address, loop=loop)
    sub = await create_redis(server.tcp_address, loop=loop)
    sub_name = 'sub-{:X}'.format(id(sub))
    await sub.client_setname(sub_name)
    for sub_info in await pub.client_list():
        if sub_info.name == sub_name:
            break
    assert sub_info.name == sub_name

    mpsc = Receiver(loop=loop)
    await sub.subscribe(mpsc.channel('channel:1'))
    await sub.subscribe(mpsc.channel('channel:2'))
    await sub.psubscribe(mpsc.pattern('channel:*'))

    q = asyncio.Queue(loop=loop)
    EOF = object()

    async def reader():
        async for ch, msg in mpsc.iter(encoding='utf-8'):
            await q.put((ch.name, msg))
        await q.put(EOF)

    tsk = asyncio.ensure_future(reader(), loop=loop)
    await pub.publish_json('channel:1', ['hello'])
    await pub.publish_json('channel:2', ['hello'])
    # receive all messages
    assert await q.get() == (b'channel:1', '["hello"]')
    assert await q.get() == (b'channel:*', (b'channel:1', '["hello"]'))
    assert await q.get() == (b'channel:2', '["hello"]')
    assert await q.get() == (b'channel:*', (b'channel:2', '["hello"]'))

    # XXX: need to implement `client kill`
    assert await pub.execute('client', 'kill', sub_info.addr) in (b'OK', 1)
    await asyncio.wait_for(tsk, timeout=1, loop=loop)
    assert await q.get() is EOF
