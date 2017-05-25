import asyncio
import pytest

from aioredis.util import create_future, async_task


@asyncio.coroutine
def _reader(channel, output, waiter, conn):
    yield from conn.execute('subscribe', channel)
    ch = conn.pubsub_channels[channel]
    waiter.set_result(conn)
    while (yield from ch.wait_message()):
        msg = yield from ch.get()
        yield from output.put(msg)


@pytest.mark.run_loop
def test_publish(create_connection, redis, server, loop):
    out = asyncio.Queue(loop=loop)
    fut = create_future(loop=loop)
    conn = yield from create_connection(
        server.tcp_address, loop=loop)
    sub = async_task(_reader('chan:1', out, fut, conn), loop=loop)

    yield from fut
    yield from redis.publish('chan:1', 'Hello')
    msg = yield from out.get()
    assert msg == b'Hello'

    sub.cancel()


@pytest.mark.run_loop
def test_publish_json(create_connection, redis, server, loop):
    out = asyncio.Queue(loop=loop)
    fut = create_future(loop=loop)
    conn = yield from create_connection(
        server.tcp_address, loop=loop)
    sub = async_task(_reader('chan:1', out, fut, conn), loop=loop)

    yield from fut

    res = yield from redis.publish_json('chan:1', {"Hello": "world"})
    assert res == 1    # recievers

    msg = yield from out.get()
    assert msg == b'{"Hello": "world"}'
    sub.cancel()


@pytest.mark.run_loop
def test_subscribe(redis):
    res = yield from redis.subscribe('chan:1', 'chan:2')
    assert redis.in_pubsub == 2

    ch1 = redis.channels['chan:1']
    ch2 = redis.channels['chan:2']

    assert res == [ch1, ch2]
    assert ch1.is_pattern is False
    assert ch2.is_pattern is False

    res = yield from redis.unsubscribe('chan:1', 'chan:2')
    assert res == [[b'unsubscribe', b'chan:1', 1],
                   [b'unsubscribe', b'chan:2', 0]]


@pytest.mark.run_loop
def test_psubscribe(redis, create_redis, server, loop):
    sub = redis
    res = yield from sub.psubscribe('patt:*', 'chan:*')
    assert sub.in_pubsub == 2

    pat1 = sub.patterns['patt:*']
    pat2 = sub.patterns['chan:*']
    assert res == [pat1, pat2]

    pub = yield from create_redis(
        server.tcp_address, loop=loop)
    yield from pub.publish_json('chan:123', {"Hello": "World"})
    res = yield from pat2.get_json()
    assert res == (b'chan:123', {"Hello": "World"})

    res = yield from sub.punsubscribe('patt:*', 'patt:*', 'chan:*')
    assert res == [[b'punsubscribe', b'patt:*', 1],
                   [b'punsubscribe', b'patt:*', 1],
                   [b'punsubscribe', b'chan:*', 0],
                   ]


@pytest.redis_version(
    2, 8, 0, reason='PUBSUB CHANNELS is available since redis>=2.8.0')
@pytest.mark.run_loop
def test_pubsub_channels(create_redis, server, loop):
    redis = yield from create_redis(
        server.tcp_address, loop=loop)
    res = yield from redis.pubsub_channels()
    assert res == []

    res = yield from redis.pubsub_channels('chan:*')
    assert res == []

    sub = yield from create_redis(
        server.tcp_address, loop=loop)
    yield from sub.subscribe('chan:1')

    res = yield from redis.pubsub_channels()
    assert res == [b'chan:1']

    res = yield from redis.pubsub_channels('ch*')
    assert res == [b'chan:1']

    yield from sub.unsubscribe('chan:1')
    yield from sub.psubscribe('chan:*')

    res = yield from redis.pubsub_channels()
    assert res == []


@pytest.redis_version(
    2, 8, 0, reason='PUBSUB NUMSUB is available since redis>=2.8.0')
@pytest.mark.run_loop
def test_pubsub_numsub(create_redis, server, loop):
    redis = yield from create_redis(
        server.tcp_address, loop=loop)
    res = yield from redis.pubsub_numsub()
    assert res == {}

    res = yield from redis.pubsub_numsub('chan:1')
    assert res == {b'chan:1': 0}

    sub = yield from create_redis(
        server.tcp_address, loop=loop)
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


@pytest.redis_version(
    2, 8, 0, reason='PUBSUB NUMPAT is available since redis>=2.8.0')
@pytest.mark.run_loop
def test_pubsub_numpat(create_redis, server, loop, redis):
    sub = yield from create_redis(
        server.tcp_address, loop=loop)

    res = yield from redis.pubsub_numpat()
    assert res == 0

    yield from sub.subscribe('chan:1')
    res = yield from redis.pubsub_numpat()
    assert res == 0

    yield from sub.psubscribe('chan:*')
    res = yield from redis.pubsub_numpat()
    assert res == 1


@pytest.mark.run_loop
def test_close_pubsub_channels(redis, loop):
    ch, = yield from redis.subscribe('chan:1')

    @asyncio.coroutine
    def waiter(ch):
        assert not (yield from ch.wait_message())

    tsk = async_task(waiter(ch), loop=loop)
    redis.close()
    yield from redis.wait_closed()
    yield from tsk


@pytest.mark.run_loop
def test_close_pubsub_patterns(redis, loop):
    ch, = yield from redis.psubscribe('chan:*')

    @asyncio.coroutine
    def waiter(ch):
        assert not (yield from ch.wait_message())

    tsk = async_task(waiter(ch), loop=loop)
    redis.close()
    yield from redis.wait_closed()
    yield from tsk


@pytest.mark.run_loop
def test_close_cancelled_pubsub_channel(redis, loop):
    ch, = yield from redis.subscribe('chan:1')

    @asyncio.coroutine
    def waiter(ch):
        with pytest.raises(asyncio.CancelledError):
            yield from ch.wait_message()

    tsk = async_task(waiter(ch), loop=loop)
    yield from asyncio.sleep(0, loop=loop)
    tsk.cancel()


@pytest.mark.run_loop
def test_channel_get_after_close(create_redis, loop, server):
    sub = yield from create_redis(
        server.tcp_address, loop=loop)
    pub = yield from create_redis(
        server.tcp_address, loop=loop)
    ch, = yield from sub.subscribe('chan:1')

    @asyncio.coroutine
    def waiter():
        while True:
            msg = yield from ch.get()
            if msg is None:
                break
            assert msg == b'message'

    tsk = async_task(waiter(), loop=loop)

    yield from pub.publish('chan:1', 'message')
    sub.close()
    yield from tsk


@pytest.mark.run_loop
def test_subscribe_concurrency(create_redis, server, loop):
    sub = yield from create_redis(
        server.tcp_address, loop=loop)
    pub = yield from create_redis(
        server.tcp_address, loop=loop)

    @asyncio.coroutine
    def subscribe(*args):
        return (yield from sub.subscribe(*args))

    @asyncio.coroutine
    def publish(*args):
        yield from asyncio.sleep(0, loop=loop)
        return (yield from pub.publish(*args))

    res = yield from asyncio.gather(
        subscribe('channel:0'),
        publish('channel:0', 'Hello'),
        subscribe('channel:1'),
        loop=loop)
    (ch1,), subs, (ch2,) = res

    assert ch1.name == b'channel:0'
    assert subs == 1
    assert ch2.name == b'channel:1'
