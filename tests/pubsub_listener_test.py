import pytest
import asyncio
import gc

from aioredis import Channel
from aioredis.pubsub import Listener


def test_make_channel(loop):
    mpsc = Listener(loop=loop)
    assert not mpsc.is_active

    ch_a = mpsc.channel("channel:1")
    assert isinstance(ch_a, Channel)
    assert mpsc.is_active

    ch_b = mpsc.channel('channel:1')
    assert ch_a is not ch_b
    assert ch_a.name == ch_b.name
    assert ch_a.is_pattern == ch_b.is_pattern
    assert mpsc.is_active

    # remember id; drop refs to objects and create new one;
    oid = id(ch_a)
    del ch_a
    del ch_b
    gc.collect()

    assert not mpsc.is_active
    ch = mpsc.channel("channel:1")
    assert id(ch) != oid


@pytest.mark.run_loop
def test_subscriptions(create_connection, server, loop):
    sub = yield from create_connection(server.tcp_address, loop=loop)
    pub = yield from create_connection(server.tcp_address, loop=loop)

    mpsc = Listener(loop=loop)
    yield from sub.execute_pubsub('subscribe',
                                  mpsc.channel('channel:1'),
                                  mpsc.channel('channel:3'))
    res = yield from pub.execute("publish", "channel:3", "Hello world")
    yield from asyncio.sleep(0, loop=loop)
    assert res == 1
    assert mpsc.is_active
    assert mpsc._queue.qsize() == 1
    ch, msg = yield from mpsc._queue.get()
    assert ch.name == b'channel:3'
    assert not ch.is_pattern
    assert msg == b"Hello world"
