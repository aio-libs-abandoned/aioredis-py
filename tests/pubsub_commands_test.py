import asyncio
import pytest
import aioredis

from _testutils import redis_version


async def _reader(channel, output, waiter, conn):
    await conn.execute('subscribe', channel)
    ch = conn.pubsub_channels[channel]
    waiter.set_result(conn)
    while await ch.wait_message():
        msg = await ch.get()
        await output.put(msg)


async def test_publish(create_connection, redis, server, loop):
    out = asyncio.Queue()
    fut = loop.create_future()
    conn = await create_connection(server.tcp_address)
    sub = asyncio.ensure_future(_reader('chan:1', out, fut, conn))

    await fut
    await redis.publish('chan:1', 'Hello')
    msg = await out.get()
    assert msg == b'Hello'

    sub.cancel()


async def test_publish_json(create_connection, redis, server, loop):
    out = asyncio.Queue()
    fut = loop.create_future()
    conn = await create_connection(server.tcp_address)
    sub = asyncio.ensure_future(_reader('chan:1', out, fut, conn))

    await fut

    res = await redis.publish_json('chan:1', {"Hello": "world"})
    assert res == 1    # receivers

    msg = await out.get()
    assert msg == b'{"Hello": "world"}'
    sub.cancel()


async def test_subscribe(redis):
    res = await redis.subscribe('chan:1', 'chan:2')
    assert redis.in_pubsub == 2

    ch1 = redis.channels['chan:1']
    ch2 = redis.channels['chan:2']

    assert res == [ch1, ch2]
    assert ch1.is_pattern is False
    assert ch2.is_pattern is False

    res = await redis.unsubscribe('chan:1', 'chan:2')
    assert res == [[b'unsubscribe', b'chan:1', 1],
                   [b'unsubscribe', b'chan:2', 0]]


@pytest.mark.parametrize('create_redis', [
    pytest.param(aioredis.create_redis_pool, id='pool'),
])
async def test_subscribe_empty_pool(create_redis, server, _closable):
    redis = await create_redis(server.tcp_address)
    _closable(redis)
    await redis.connection.clear()

    res = await redis.subscribe('chan:1', 'chan:2')
    assert redis.in_pubsub == 2

    ch1 = redis.channels['chan:1']
    ch2 = redis.channels['chan:2']

    assert res == [ch1, ch2]
    assert ch1.is_pattern is False
    assert ch2.is_pattern is False

    res = await redis.unsubscribe('chan:1', 'chan:2')
    assert res == [[b'unsubscribe', b'chan:1', 1],
                   [b'unsubscribe', b'chan:2', 0]]


async def test_psubscribe(redis, create_redis, server):
    sub = redis
    res = await sub.psubscribe('patt:*', 'chan:*')
    assert sub.in_pubsub == 2

    pat1 = sub.patterns['patt:*']
    pat2 = sub.patterns['chan:*']
    assert res == [pat1, pat2]

    pub = await create_redis(server.tcp_address)
    await pub.publish_json('chan:123', {"Hello": "World"})
    res = await pat2.get_json()
    assert res == (b'chan:123', {"Hello": "World"})

    res = await sub.punsubscribe('patt:*', 'patt:*', 'chan:*')
    assert res == [[b'punsubscribe', b'patt:*', 1],
                   [b'punsubscribe', b'patt:*', 1],
                   [b'punsubscribe', b'chan:*', 0],
                   ]


@pytest.mark.parametrize('create_redis', [
    pytest.param(aioredis.create_redis_pool, id='pool'),
])
async def test_psubscribe_empty_pool(create_redis, server, _closable):
    sub = await create_redis(server.tcp_address)
    pub = await create_redis(server.tcp_address)
    _closable(sub)
    _closable(pub)
    await sub.connection.clear()
    res = await sub.psubscribe('patt:*', 'chan:*')
    assert sub.in_pubsub == 2

    pat1 = sub.patterns['patt:*']
    pat2 = sub.patterns['chan:*']
    assert res == [pat1, pat2]

    await pub.publish_json('chan:123', {"Hello": "World"})
    res = await pat2.get_json()
    assert res == (b'chan:123', {"Hello": "World"})

    res = await sub.punsubscribe('patt:*', 'patt:*', 'chan:*')
    assert res == [[b'punsubscribe', b'patt:*', 1],
                   [b'punsubscribe', b'patt:*', 1],
                   [b'punsubscribe', b'chan:*', 0],
                   ]


@redis_version(
    2, 8, 0, reason='PUBSUB CHANNELS is available since redis>=2.8.0')
async def test_pubsub_channels(create_redis, server):
    redis = await create_redis(server.tcp_address)
    res = await redis.pubsub_channels()
    assert res == []

    res = await redis.pubsub_channels('chan:*')
    assert res == []

    sub = await create_redis(server.tcp_address)
    await sub.subscribe('chan:1')

    res = await redis.pubsub_channels()
    assert res == [b'chan:1']

    res = await redis.pubsub_channels('ch*')
    assert res == [b'chan:1']

    await sub.unsubscribe('chan:1')
    await sub.psubscribe('chan:*')

    res = await redis.pubsub_channels()
    assert res == []


@redis_version(
    2, 8, 0, reason='PUBSUB NUMSUB is available since redis>=2.8.0')
async def test_pubsub_numsub(create_redis, server):
    redis = await create_redis(server.tcp_address)
    res = await redis.pubsub_numsub()
    assert res == {}

    res = await redis.pubsub_numsub('chan:1')
    assert res == {b'chan:1': 0}

    sub = await create_redis(server.tcp_address)
    await sub.subscribe('chan:1')

    res = await redis.pubsub_numsub()
    assert res == {}

    res = await redis.pubsub_numsub('chan:1')
    assert res == {b'chan:1': 1}

    res = await redis.pubsub_numsub('chan:2')
    assert res == {b'chan:2': 0}

    res = await redis.pubsub_numsub('chan:1', 'chan:2')
    assert res == {b'chan:1': 1, b'chan:2': 0}

    await sub.unsubscribe('chan:1')
    await sub.psubscribe('chan:*')

    res = await redis.pubsub_numsub()
    assert res == {}


@redis_version(
    2, 8, 0, reason='PUBSUB NUMPAT is available since redis>=2.8.0')
async def test_pubsub_numpat(create_redis, server, redis):
    sub = await create_redis(server.tcp_address)

    res = await redis.pubsub_numpat()
    assert res == 0

    await sub.subscribe('chan:1')
    res = await redis.pubsub_numpat()
    assert res == 0

    await sub.psubscribe('chan:*')
    res = await redis.pubsub_numpat()
    assert res == 1


async def test_close_pubsub_channels(redis):
    ch, = await redis.subscribe('chan:1')

    async def waiter(ch):
        assert not await ch.wait_message()

    tsk = asyncio.ensure_future(waiter(ch))
    redis.close()
    await redis.wait_closed()
    await tsk


async def test_close_pubsub_patterns(redis):
    ch, = await redis.psubscribe('chan:*')

    async def waiter(ch):
        assert not await ch.wait_message()

    tsk = asyncio.ensure_future(waiter(ch))
    redis.close()
    await redis.wait_closed()
    await tsk


async def test_close_cancelled_pubsub_channel(redis):
    ch, = await redis.subscribe('chan:1')

    async def waiter(ch):
        with pytest.raises(asyncio.CancelledError):
            await ch.wait_message()

    tsk = asyncio.ensure_future(waiter(ch))
    await asyncio.sleep(0)
    tsk.cancel()


async def test_channel_get_after_close(create_redis, loop, server):
    sub = await create_redis(server.tcp_address)
    pub = await create_redis(server.tcp_address)
    ch, = await sub.subscribe('chan:1')

    await pub.publish('chan:1', 'message')
    assert await ch.get() == b'message'
    loop.call_soon(sub.close)
    assert await ch.get() is None
    with pytest.raises(aioredis.ChannelClosedError):
        assert await ch.get()


async def test_subscribe_concurrency(create_redis, server):
    sub = await create_redis(server.tcp_address)
    pub = await create_redis(server.tcp_address)

    async def subscribe(*args):
        return await sub.subscribe(*args)

    async def publish(*args):
        await asyncio.sleep(0)
        return await pub.publish(*args)

    res = await asyncio.gather(
        subscribe('channel:0'),
        publish('channel:0', 'Hello'),
        subscribe('channel:1'),
        )
    (ch1,), subs, (ch2,) = res

    assert ch1.name == b'channel:0'
    assert subs == 1
    assert ch2.name == b'channel:1'


@redis_version(
    3, 2, 0, reason='PUBSUB PING is available since redis>=3.2.0')
async def test_pubsub_ping(redis):
    await redis.subscribe('chan:1', 'chan:2')

    res = await redis.ping()
    assert res == b'PONG'
    res = await redis.ping('Hello')
    assert res == b'Hello'
    res = await redis.ping('Hello', encoding='utf-8')
    assert res == 'Hello'

    await redis.unsubscribe('chan:1', 'chan:2')


async def test_pubsub_channel_iter(create_redis, server):
    sub = await create_redis(server.tcp_address)
    pub = await create_redis(server.tcp_address)

    ch, = await sub.subscribe('chan:1')

    async def coro(ch):
        lst = []
        async for msg in ch.iter():
            lst.append(msg)
        return lst

    tsk = asyncio.ensure_future(coro(ch))
    await pub.publish_json('chan:1', {'Hello': 'World'})
    await pub.publish_json('chan:1', ['message'])
    await asyncio.sleep(0.1)
    ch.close()
    assert await tsk == [b'{"Hello": "World"}', b'["message"]']


@redis_version(
    2, 8, 12, reason="extended `client kill` format required")
async def test_pubsub_disconnection_notification(create_redis, server):
    sub = await create_redis(server.tcp_address)
    pub = await create_redis(server.tcp_address)

    async def coro(ch):
        lst = []
        async for msg in ch.iter():
            assert ch.is_active
            lst.append(msg)
        return lst

    ch, = await sub.subscribe('chan:1')
    tsk = asyncio.ensure_future(coro(ch))
    assert ch.is_active
    await pub.publish_json('chan:1', {'Hello': 'World'})
    assert ch.is_active
    assert await pub.execute('client', 'kill', 'type', 'pubsub') >= 1
    assert await pub.publish_json('chan:1', ['message']) == 0
    assert await tsk == [b'{"Hello": "World"}']
    assert not ch.is_active
