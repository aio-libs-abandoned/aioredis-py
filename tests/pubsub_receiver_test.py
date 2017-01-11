import pytest
import asyncio
import json
import sys

from unittest import mock

from aioredis import ChannelClosedError
from aioredis.abc import AbcChannel
from aioredis.pubsub import Receiver, _Sender
from aioredis.util import async_task


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
def test_sender(loop):
    receiver = mock.Mock()

    sender = _Sender(receiver, 'name', is_pattern=False, loop=loop)
    assert isinstance(sender, AbcChannel)
    assert sender.name == b'name'
    assert sender.is_pattern is False
    assert sender.is_active is True

    with pytest.raises(RuntimeError):
        yield from sender.get()
    assert receiver.mock_calls == []

    sender.put_nowait(b'some data')
    assert receiver.mock_calls == [
        mock.call._put_nowait(b'some data', sender=sender),
        ]


def test_sender_close(loop):
    receiver = mock.Mock()
    sender = _Sender(receiver, 'name', is_pattern=False, loop=loop)
    sender.close()
    assert receiver.mock_calls == [mock.call._close(sender)]
    sender.close()
    assert receiver.mock_calls == [mock.call._close(sender)]
    receiver.reset_mock()
    assert receiver.mock_calls == []
    sender.close()
    assert receiver.mock_calls == []


@pytest.mark.run_loop
def test_subscriptions(create_connection, server, loop):
    sub = yield from create_connection(server.tcp_address, loop=loop)
    pub = yield from create_connection(server.tcp_address, loop=loop)

    mpsc = Receiver(loop=loop)
    yield from sub.execute_pubsub('subscribe',
                                  mpsc.channel('channel:1'),
                                  mpsc.channel('channel:3'))
    res = yield from pub.execute("publish", "channel:3", "Hello world")
    assert res == 1
    res = yield from pub.execute("publish", "channel:1", "Hello world")
    assert res == 1
    assert mpsc.is_active

    ch, msg = yield from mpsc.get()
    assert ch.name == b'channel:3'
    assert not ch.is_pattern
    assert msg == b"Hello world"

    ch, msg = yield from mpsc.get()
    assert ch.name == b'channel:1'
    assert not ch.is_pattern
    assert msg == b"Hello world"


@pytest.mark.run_loop
def test_unsubscribe(create_connection, server, loop):
    sub = yield from create_connection(server.tcp_address, loop=loop)
    pub = yield from create_connection(server.tcp_address, loop=loop)

    mpsc = Receiver(loop=loop)
    yield from sub.execute_pubsub('subscribe',
                                  mpsc.channel('channel:1'),
                                  mpsc.channel('channel:3'))
    res = yield from pub.execute("publish", "channel:3", "Hello world")
    assert res == 1
    res = yield from pub.execute("publish", "channel:1", "Hello world")
    assert res == 1
    assert mpsc.is_active

    assert (yield from mpsc.wait_message()) is True
    ch, msg = yield from mpsc.get()
    assert ch.name == b'channel:3'
    assert not ch.is_pattern
    assert msg == b"Hello world"

    assert (yield from mpsc.wait_message()) is True
    ch, msg = yield from mpsc.get()
    assert ch.name == b'channel:1'
    assert not ch.is_pattern
    assert msg == b"Hello world"

    yield from sub.execute_pubsub('unsubscribe', 'channel:1')
    assert mpsc.is_active

    res = yield from pub.execute("publish", "channel:3", "message")
    assert res == 1
    assert (yield from mpsc.wait_message()) is True
    ch, msg = yield from mpsc.get()
    assert ch.name == b'channel:3'
    assert not ch.is_pattern
    assert msg == b"message"

    yield from sub.execute_pubsub('unsubscribe', 'channel:3')
    assert not mpsc.is_active
    res = yield from mpsc.get()
    assert res is None


@pytest.mark.run_loop
def test_stopped(create_connection, server, loop):
    sub = yield from create_connection(server.tcp_address, loop=loop)
    pub = yield from create_connection(server.tcp_address, loop=loop)

    mpsc = Receiver(loop=loop)
    yield from sub.execute_pubsub('subscribe', mpsc.channel('channel:1'))
    assert mpsc.is_active
    mpsc.stop()

    with pytest.logs('aioredis', 'DEBUG') as cm:
        yield from pub.execute('publish', 'channel:1', b'Hello')
        yield from asyncio.sleep(0, loop=loop)

    assert len(cm.output) == 1
    warn_messaege = (
        "WARNING:aioredis:Pub/Sub listener message after stop: "
        "<_Sender name:b'channel:1', is_pattern:False, receiver:"
        "<Receiver is_active:False, senders:1, qsize:0>>, b'Hello'"
    )
    assert cm.output == [warn_messaege]

    with pytest.raises(ChannelClosedError):
        yield from mpsc.get()
    res = yield from mpsc.wait_message()
    assert res is False


@pytest.mark.run_loop
def test_wait_message(create_connection, server, loop):
    sub = yield from create_connection(server.tcp_address, loop=loop)
    pub = yield from create_connection(server.tcp_address, loop=loop)

    mpsc = Receiver(loop=loop)
    yield from sub.execute_pubsub('subscribe', mpsc.channel('channel:1'))
    fut = async_task(mpsc.wait_message(), loop=loop)
    assert not fut.done()
    yield from asyncio.sleep(0, loop=loop)
    assert not fut.done()

    yield from pub.execute('publish', 'channel:1', 'hello')
    yield from asyncio.sleep(0, loop=loop)  # read in connection
    yield from asyncio.sleep(0, loop=loop)  # call Future.set_result
    assert fut.done()
    res = yield from fut
    assert res is True


@pytest.mark.run_loop
def test_decode_message(loop):
    mpsc = Receiver(loop)
    ch = mpsc.channel('channel:1')
    ch.put_nowait(b'Some data')

    res = yield from mpsc.get(encoding='utf-8')
    assert isinstance(res[0], _Sender)
    assert res[1] == 'Some data'

    ch.put_nowait('{"hello": "world"}')
    res = yield from mpsc.get(decoder=json.loads)
    assert isinstance(res[0], _Sender)
    assert res[1] == {'hello': 'world'}

    ch.put_nowait(b'{"hello": "world"}')
    res = yield from mpsc.get(encoding='utf-8', decoder=json.loads)
    assert isinstance(res[0], _Sender)
    assert res[1] == {'hello': 'world'}


@pytest.mark.skipif(sys.version_info >= (3, 6),
                    reason="json.loads accept bytes since Python 3.6")
@pytest.mark.run_loop
def test_decode_message_error(loop):
    mpsc = Receiver(loop)
    ch = mpsc.channel('channel:1')

    ch.put_nowait(b'{"hello": "world"}')
    unexpected = (mock.ANY, {'hello': 'world'})
    with pytest.raises(TypeError):
        assert (yield from mpsc.get(decoder=json.loads)) == unexpected

    ch = mpsc.pattern('*')
    ch.put_nowait((b'channel', b'{"hello": "world"}'))
    unexpected = (mock.ANY, b'channel', {'hello': 'world'})
    with pytest.raises(TypeError):
        assert (yield from mpsc.get(decoder=json.loads)) == unexpected


@pytest.mark.run_loop
def test_decode_message_for_pattern(loop):
    mpsc = Receiver(loop)
    ch = mpsc.pattern('*')
    ch.put_nowait((b'channel', b'Some data'))

    res = yield from mpsc.get(encoding='utf-8')
    assert isinstance(res[0], _Sender)
    assert res[1] == (b'channel', 'Some data')

    ch.put_nowait((b'channel', '{"hello": "world"}'))
    res = yield from mpsc.get(decoder=json.loads)
    assert isinstance(res[0], _Sender)
    assert res[1] == (b'channel', {'hello': 'world'})

    ch.put_nowait((b'channel', b'{"hello": "world"}'))
    res = yield from mpsc.get(encoding='utf-8', decoder=json.loads)
    assert isinstance(res[0], _Sender)
    assert res[1] == (b'channel', {'hello': 'world'})
