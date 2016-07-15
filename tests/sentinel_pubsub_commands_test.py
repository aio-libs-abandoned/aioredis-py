import asyncio
import pytest
from unittest import mock

master_name = 'XXX'
redis_sentinel = mock.Mock()
get_master_connection = mock.Mock()
create_connection = mock.Mock()


@asyncio.coroutine
def _reader(channel, output, waiter, conn=None, loop=None):
    if conn is None:
        master = yield from redis_sentinel.discover_master(
            master_name)
        conn = yield from create_connection(
            master, loop=loop)
    yield from conn.execute('subscribe', channel)
    ch = conn.pubsub_channels[channel]
    waiter.set_result(conn)
    while (yield from ch.wait_message()):
        msg = yield from ch.get()
        yield from output.put(msg)


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.mark.run_loop
def test_publish(loop):
    out = asyncio.Queue(loop=loop)
    fut = asyncio.Future(loop=loop)
    sub = asyncio.async(_reader('chan:1', out, fut, loop=loop),
                        loop=loop)

    redis = yield from get_master_connection()

    yield from fut
    yield from redis.publish('chan:1', 'Hello')
    msg = yield from out.get()
    assert msg == b'Hello'

    sub.cancel()


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.mark.run_loop
def test_publish_json(loop):
    out = asyncio.Queue(loop=loop)
    fut = asyncio.Future(loop=loop)
    sub = asyncio.async(_reader('chan:1', out, fut, loop=loop),
                        loop=loop)

    redis = yield from get_master_connection()
    yield from fut

    res = yield from redis.publish_json('chan:1', {"Hello": "world"})
    assert res == 1    # recievers

    msg = yield from out.get()
    assert msg == b'{"Hello": "world"}'
    sub.cancel()


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.mark.run_loop
def test_subscribe():
    sub = yield from get_master_connection()
    res = yield from sub.subscribe('chan:1', 'chan:2')
    assert sub.in_pubsub == 2

    ch1 = sub.channels['chan:1']
    ch2 = sub.channels['chan:2']

    assert res == [ch1, ch2]
    assert ch1.is_pattern is False
    assert ch2.is_pattern is False

    res = yield from sub.unsubscribe('chan:1', 'chan:2')
    assert res == [[b'unsubscribe', b'chan:1', 1],
                   [b'unsubscribe', b'chan:2', 0]]


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.mark.run_loop
def test_psubscribe():
    sub = yield from get_master_connection()
    res = yield from sub.psubscribe('patt:*', 'chan:*')
    assert sub.in_pubsub == 2

    pat1 = sub.patterns['patt:*']
    pat2 = sub.patterns['chan:*']
    assert res == [pat1, pat2]

    pub = yield from get_master_connection()
    yield from pub.publish_json('chan:123', {"Hello": "World"})
    res = yield from pat2.get_json()
    assert res == (b'chan:123', {"Hello": "World"})

    res = yield from sub.punsubscribe('patt:*', 'patt:*', 'chan:*')
    assert res == [[b'punsubscribe', b'patt:*', 1],
                   [b'punsubscribe', b'patt:*', 1],
                   [b'punsubscribe', b'chan:*', 0],
                   ]


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.redis_version(
    2, 8, 0, reason='PUBSUB CHANNELS is available since redis>=2.8.0')
@pytest.mark.run_loop
def test_pubsub_channels():
    redis = yield from get_master_connection()
    res = yield from redis.pubsub_channels()
    assert res == [b'__sentinel__:hello']

    res = yield from redis.pubsub_channels('chan:*')
    assert res == []

    sub = yield from get_master_connection()
    yield from sub.subscribe('chan:1')

    res = yield from redis.pubsub_channels()
    assert set(res) == {b'__sentinel__:hello', b'chan:1'}

    res = yield from redis.pubsub_channels('ch*')
    assert res == [b'chan:1']

    yield from sub.unsubscribe('chan:1')
    yield from sub.psubscribe('chan:*')

    res = yield from redis.pubsub_channels()
    assert res == [b'__sentinel__:hello']


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.redis_version(
    2, 8, 0, reason='PUBSUB NUMSUB is available since redis>=2.8.0')
@pytest.mark.run_loop
def test_pubsub_numsub():
    redis = yield from get_master_connection()
    res = yield from redis.pubsub_numsub()
    assert res == {}

    res = yield from redis.pubsub_numsub('chan:1')
    assert res == {b'chan:1': 0}

    sub = yield from get_master_connection()
    yield from sub.subscribe('chan:1')

    res = yield from redis.pubsub_numsub()
    assert res == {}

    res = yield from redis.pubsub_numsub('chan:1')
    assert res == {b'chan:1': 1}

    res = yield from redis.pubsub_numsub('chan:2')
    assert res == {b'chan:2': 0}

    res = yield from redis.pubsub_numsub('chan:1', 'chan:2')
    assert res == {b'chan:1': 1, b'chan:2': 0}

    yield from sub.unsubscribe('chan:1')
    yield from sub.psubscribe('chan:*')

    res = yield from redis.pubsub_numsub()
    assert res == {}


@pytest.mark.xfail(reason="Not ported to pytest")
@pytest.redis_version(
    2, 8, 0, reason='PUBSUB NUMPAT is available since redis>=2.8.0')
@pytest.mark.run_loop
def test_pubsub_numpat():
    redis = yield from get_master_connection()
    sub = yield from get_master_connection()

    res = yield from redis.pubsub_numpat()
    assert res == 0

    yield from sub.subscribe('chan:1')
    res = yield from redis.pubsub_numpat()
    assert res == 0

    yield from sub.psubscribe('chan:*')
    res = yield from redis.pubsub_numpat()
    assert res == 1
