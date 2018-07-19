import asyncio
import pytest
import async_timeout

from unittest.mock import patch

from aioredis import (
    ReplyError,
    PoolClosedError,
    ConnectionClosedError,
    ConnectionsPool,
    MaxClientsError,
    )


def _assert_defaults(pool):
    assert isinstance(pool, ConnectionsPool)
    assert pool.minsize == 1
    assert pool.maxsize == 10
    assert pool.size == 1
    assert pool.freesize == 1
    assert pool._close_waiter is None


def test_connect(pool):
    _assert_defaults(pool)


def test_global_loop(create_pool, loop, server):
    asyncio.set_event_loop(loop)

    pool = loop.run_until_complete(create_pool(
        server.tcp_address))
    _assert_defaults(pool)


@pytest.mark.run_loop
async def test_clear(pool):
    _assert_defaults(pool)

    await pool.clear()
    assert pool.freesize == 0


@pytest.mark.run_loop
@pytest.mark.parametrize('minsize', [None, -100, 0.0, 100])
async def test_minsize(minsize, create_pool, loop, server):

    with pytest.raises(AssertionError):
        await create_pool(
            server.tcp_address,
            minsize=minsize, maxsize=10, loop=loop)


@pytest.mark.run_loop
@pytest.mark.parametrize('maxsize', [None, -100, 0.0, 1])
async def test_maxsize(maxsize, create_pool, loop, server):

    with pytest.raises(AssertionError):
        await create_pool(
            server.tcp_address,
            minsize=2, maxsize=maxsize, loop=loop)


@pytest.mark.run_loop
async def test_create_connection_timeout(create_pool, loop, server):
    with patch.object(loop, 'create_connection') as\
            open_conn_mock:
        open_conn_mock.side_effect = lambda *a, **kw: asyncio.sleep(0.2,
                                                                    loop=loop)
        with pytest.raises(asyncio.TimeoutError):
            await create_pool(
                server.tcp_address, loop=loop,
                create_connection_timeout=0.1)


def test_no_yield_from(pool):
    with pytest.raises(RuntimeError):
        with pool:
            pass    # pragma: no cover


@pytest.mark.run_loop
async def test_simple_command(create_pool, loop, server):
    pool = await create_pool(
        server.tcp_address,
        minsize=10, loop=loop)

    with (await pool) as conn:
        msg = await conn.execute('echo', 'hello')
        assert msg == b'hello'
        assert pool.size == 10
        assert pool.freesize == 9
    assert pool.size == 10
    assert pool.freesize == 10


@pytest.mark.run_loop
async def test_create_new(create_pool, loop, server):
    pool = await create_pool(
        server.tcp_address,
        minsize=1, loop=loop)
    assert pool.size == 1
    assert pool.freesize == 1

    with (await pool):
        assert pool.size == 1
        assert pool.freesize == 0

        with (await pool):
            assert pool.size == 2
            assert pool.freesize == 0

    assert pool.size == 2
    assert pool.freesize == 2


@pytest.mark.run_loop
async def test_create_constraints(create_pool, loop, server):
    pool = await create_pool(
        server.tcp_address,
        minsize=1, maxsize=1, loop=loop)
    assert pool.size == 1
    assert pool.freesize == 1

    with (await pool):
        assert pool.size == 1
        assert pool.freesize == 0

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(pool.acquire(),
                                   timeout=0.2,
                                   loop=loop)


@pytest.mark.run_loop
async def test_create_no_minsize(create_pool, loop, server):
    pool = await create_pool(
        server.tcp_address,
        minsize=0, maxsize=1, loop=loop)
    assert pool.size == 0
    assert pool.freesize == 0

    with (await pool):
        assert pool.size == 1
        assert pool.freesize == 0

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(pool.acquire(),
                                   timeout=0.2,
                                   loop=loop)
    assert pool.size == 1
    assert pool.freesize == 1


@pytest.mark.run_loop
async def test_create_pool_cls(create_pool, loop, server):

    class MyPool(ConnectionsPool):
        pass

    pool = await create_pool(
        server.tcp_address,
        loop=loop,
        pool_cls=MyPool)

    assert isinstance(pool, MyPool)


@pytest.mark.run_loop
async def test_create_pool_cls_invalid(create_pool, loop, server):
    with pytest.raises(AssertionError):
        await create_pool(
            server.tcp_address,
            loop=loop,
            pool_cls=type)


@pytest.mark.run_loop
async def test_release_closed(create_pool, loop, server):
    pool = await create_pool(
        server.tcp_address,
        minsize=1, loop=loop)
    assert pool.size == 1
    assert pool.freesize == 1

    with (await pool) as conn:
        conn.close()
        await conn.wait_closed()
    assert pool.size == 0
    assert pool.freesize == 0


@pytest.mark.run_loop
async def test_release_pending(create_pool, loop, server):
    pool = await create_pool(
        server.tcp_address,
        minsize=1, loop=loop)
    assert pool.size == 1
    assert pool.freesize == 1

    with pytest.logs('aioredis', 'WARNING') as cm:
        with (await pool) as conn:
            try:
                await asyncio.wait_for(
                    conn.execute(
                        b'blpop',
                        b'somekey:not:exists',
                        b'0'),
                    0.1,
                    loop=loop)
            except asyncio.TimeoutError:
                pass
    assert pool.size == 0
    assert pool.freesize == 0
    assert cm.output == [
        'WARNING:aioredis:Connection <RedisConnection [db:0]>'
        ' has pending commands, closing it.'
    ]


@pytest.mark.run_loop
async def test_release_bad_connection(create_pool, create_redis, loop, server):
    pool = await create_pool(
        server.tcp_address,
        loop=loop)
    conn = await pool.acquire()
    assert conn.address[0] in ('127.0.0.1', '::1')
    assert conn.address[1] == server.tcp_address.port
    other_conn = await create_redis(
        server.tcp_address,
        loop=loop)
    with pytest.raises(AssertionError):
        pool.release(other_conn)

    pool.release(conn)
    other_conn.close()
    await other_conn.wait_closed()


@pytest.mark.run_loop
async def test_select_db(create_pool, loop, server):
    pool = await create_pool(
        server.tcp_address,
        loop=loop)

    await pool.select(1)
    with (await pool) as conn:
        assert conn.db == 1


@pytest.mark.run_loop
async def test_change_db(create_pool, loop, server):
    pool = await create_pool(
        server.tcp_address,
        minsize=1, db=0,
        loop=loop)
    assert pool.size == 1
    assert pool.freesize == 1

    with (await pool) as conn:
        await conn.select(1)
    assert pool.size == 0
    assert pool.freesize == 0

    with (await pool):
        assert pool.size == 1
        assert pool.freesize == 0

        await pool.select(1)
        assert pool.db == 1
        assert pool.size == 1
        assert pool.freesize == 0
    assert pool.size == 0
    assert pool.freesize == 0
    assert pool.db == 1


@pytest.mark.run_loop
async def test_change_db_errors(create_pool, loop, server):
    pool = await create_pool(
        server.tcp_address,
        minsize=1, db=0,
        loop=loop)

    with pytest.raises(TypeError):
        await pool.select(None)
    assert pool.db == 0

    with (await pool):
        pass
    assert pool.size == 1
    assert pool.freesize == 1

    with pytest.raises(TypeError):
        await pool.select(None)
    assert pool.db == 0
    with pytest.raises(ValueError):
        await pool.select(-1)
    assert pool.db == 0
    with pytest.raises(ReplyError):
        await pool.select(100000)
    assert pool.db == 0


@pytest.mark.xfail(reason="Need to refactor this test")
@pytest.mark.run_loop
async def test_select_and_create(create_pool, loop, server):
    # trying to model situation when select and acquire
    # called simultaneously
    # but acquire freezes on _wait_select and
    # then continues with propper db

    # TODO: refactor this test as there's no _wait_select any more.
    with async_timeout.timeout(10, loop=loop):
        pool = await create_pool(
            server.tcp_address,
            minsize=1, db=0,
            loop=loop)
        db = 0
        while True:
            db = (db + 1) & 1
            _, conn = await asyncio.gather(pool.select(db),
                                           pool.acquire(),
                                           loop=loop)
            assert pool.db == db
            pool.release(conn)
            if conn.db == db:
                break
    # await asyncio.wait_for(test(), 3, loop=loop)


@pytest.mark.run_loop
async def test_response_decoding(create_pool, loop, server):
    pool = await create_pool(
        server.tcp_address,
        encoding='utf-8', loop=loop)

    assert pool.encoding == 'utf-8'
    with (await pool) as conn:
        await conn.execute('set', 'key', 'value')
    with (await pool) as conn:
        res = await conn.execute('get', 'key')
        assert res == 'value'


@pytest.mark.run_loop
async def test_hgetall_response_decoding(create_pool, loop, server):
    pool = await create_pool(
        server.tcp_address,
        encoding='utf-8', loop=loop)

    assert pool.encoding == 'utf-8'
    with (await pool) as conn:
        await conn.execute('del', 'key1')
        await conn.execute('hmset', 'key1', 'foo', 'bar')
        await conn.execute('hmset', 'key1', 'baz', 'zap')
    with (await pool) as conn:
        res = await conn.execute('hgetall', 'key1')
        assert res == ['foo', 'bar', 'baz', 'zap']


@pytest.mark.run_loop
async def test_crappy_multiexec(create_pool, loop, server):
    pool = await create_pool(
        server.tcp_address,
        encoding='utf-8', loop=loop,
        minsize=1, maxsize=1)

    with (await pool) as conn:
        await conn.execute('set', 'abc', 'def')
        await conn.execute('multi')
        await conn.execute('set', 'abc', 'fgh')
    assert conn.closed is True
    with (await pool) as conn:
        value = await conn.execute('get', 'abc')
    assert value == 'def'


@pytest.mark.run_loop
async def test_pool_size_growth(create_pool, server, loop):
    pool = await create_pool(
        server.tcp_address,
        loop=loop,
        minsize=1, maxsize=1)

    done = set()
    tasks = []

    async def task1(i):
        with (await pool):
            assert pool.size <= pool.maxsize
            assert pool.freesize == 0
            await asyncio.sleep(0.2, loop=loop)
            done.add(i)

    async def task2():
        with (await pool):
            assert pool.size <= pool.maxsize
            assert pool.freesize >= 0
            assert done == {0, 1}

    for _ in range(2):
        tasks.append(asyncio.ensure_future(task1(_), loop=loop))
    tasks.append(asyncio.ensure_future(task2(), loop=loop))
    await asyncio.gather(*tasks, loop=loop)


@pytest.mark.run_loop
async def test_pool_with_closed_connections(create_pool, server, loop):
    pool = await create_pool(
        server.tcp_address,
        loop=loop,
        minsize=1, maxsize=2)
    assert 1 == pool.freesize
    conn1 = pool._pool[0]
    conn1.close()
    assert conn1.closed is True
    assert 1 == pool.freesize
    with (await pool) as conn2:
        assert conn2.closed is False
        assert conn1 is not conn2


@pytest.mark.run_loop
async def test_pool_close(create_pool, server, loop):
    pool = await create_pool(
        server.tcp_address, loop=loop)

    assert pool.closed is False

    with (await pool) as conn:
        assert (await conn.execute('ping')) == b'PONG'

    pool.close()
    await pool.wait_closed()
    assert pool.closed is True

    with pytest.raises(PoolClosedError):
        with (await pool) as conn:
            assert (await conn.execute('ping')) == b'PONG'


@pytest.mark.run_loop
async def test_pool_close__used(create_pool, server, loop):
    pool = await create_pool(
        server.tcp_address, loop=loop)

    assert pool.closed is False

    with (await pool) as conn:
        pool.close()
        await pool.wait_closed()
        assert pool.closed is True

        with pytest.raises(ConnectionClosedError):
            await conn.execute('ping')


@pytest.mark.run_loop
@pytest.redis_version(2, 8, 0, reason="maxclients config setting")
async def test_pool_check_closed_when_exception(
        create_pool, create_redis, start_server, loop):
    server = start_server('server-small')
    redis = await create_redis(server.tcp_address, loop=loop)
    await redis.config_set('maxclients', 2)

    errors = (MaxClientsError, ConnectionClosedError, ConnectionError)
    with pytest.logs('aioredis', 'DEBUG') as cm:
        with pytest.raises(errors):
            await create_pool(address=tuple(server.tcp_address),
                              minsize=3, loop=loop)

    assert len(cm.output) >= 3
    connect_msg = (
        "DEBUG:aioredis:Creating tcp connection"
        " to ('localhost', {})".format(server.tcp_address.port))
    assert cm.output[:2] == [connect_msg, connect_msg]
    assert cm.output[-1] == "DEBUG:aioredis:Closed 1 connection(s)"


@pytest.mark.run_loop
async def test_pool_get_connection(create_pool, server, loop):
    pool = await create_pool(server.tcp_address, minsize=1, maxsize=2,
                             loop=loop)
    res = await pool.execute("set", "key", "val")
    assert res == b'OK'

    res = await pool.execute_pubsub("subscribe", "channel:1")
    assert res == [[b"subscribe", b"channel:1", 1]]

    res = await pool.execute("getset", "key", "value")
    assert res == b'val'

    res = await pool.execute_pubsub("subscribe", "channel:2")
    assert res == [[b"subscribe", b"channel:2", 2]]

    res = await pool.execute("get", "key")
    assert res == b'value'


@pytest.mark.run_loop
async def test_pool_get_connection_with_pipelining(create_pool, server, loop):
    pool = await create_pool(server.tcp_address, minsize=1, maxsize=2,
                             loop=loop)
    fut1 = pool.execute('set', 'key', 'val')
    fut2 = pool.execute_pubsub("subscribe", "channel:1")
    fut3 = pool.execute('getset', 'key', 'next')
    fut4 = pool.execute_pubsub("subscribe", "channel:2")
    fut5 = pool.execute('get', 'key')
    res = await fut1
    assert res == b'OK'
    res = await fut2
    assert res == [[b"subscribe", b"channel:1", 1]]
    res = await fut3
    assert res == b'val'
    res = await fut4
    assert res == [[b"subscribe", b"channel:2", 2]]
    res = await fut5
    assert res == b'next'


@pytest.mark.run_loop
async def test_pool_idle_close(create_pool, start_server, loop):
    server = start_server('idle')
    conn = await create_pool(server.tcp_address, minsize=2, loop=loop)
    ok = await conn.execute("config", "set", "timeout", 1)
    assert ok == b'OK'

    await asyncio.sleep(2, loop=loop)

    assert (await conn.execute('ping')) == b'PONG'


@pytest.mark.run_loop
async def test_await(create_pool, server, loop):
    pool = await create_pool(
        server.tcp_address,
        minsize=10, loop=loop)

    with (await pool) as conn:
        msg = await conn.execute('echo', 'hello')
        assert msg == b'hello'


@pytest.mark.run_loop
async def test_async_with(create_pool, server, loop):
    pool = await create_pool(
        server.tcp_address,
        minsize=10, loop=loop)

    async with pool.get() as conn:
        msg = await conn.execute('echo', 'hello')
        assert msg == b'hello'
