import asyncio
import pytest

from unittest.mock import patch

from aioredis import (
    RedisPool,
    ReplyError,
    PoolClosedError,
    ConnectionClosedError,
    )


def _assert_defaults(pool):
    assert isinstance(pool, RedisPool)
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
def test_clear(pool):
    _assert_defaults(pool)

    yield from pool.clear()
    assert pool.freesize == 0


@pytest.mark.run_loop
@pytest.mark.parametrize('minsize', [None, -100, 0.0, 100])
def test_minsize(minsize, create_pool, loop, server):

    with pytest.raises(AssertionError):
        yield from create_pool(
            server.tcp_address,
            minsize=minsize, maxsize=10, loop=loop)


@pytest.mark.run_loop
@pytest.mark.parametrize('maxsize', [None, -100, 0.0, 1])
def test_maxsize(maxsize, create_pool, loop, server):

    with pytest.raises(AssertionError):
        yield from create_pool(
            server.tcp_address,
            minsize=2, maxsize=maxsize, loop=loop)


@pytest.mark.run_loop
def test_create_connection_timeout(create_pool, loop, server):
    with patch('asyncio.open_connection') as\
            open_conn_mock:
        open_conn_mock.side_effect = lambda *a, **kw: asyncio.sleep(0.2,
                                                                    loop=loop)
        with pytest.raises(asyncio.TimeoutError):
            yield from create_pool(
                server.tcp_address, loop=loop,
                create_connection_timeout=0.1)


def test_no_yield_from(pool):
    with pytest.raises(RuntimeError):
        with pool:
            pass    # pragma: no cover


@pytest.mark.run_loop
def test_simple_command(create_pool, loop, server):
    pool = yield from create_pool(
        server.tcp_address,
        minsize=10, loop=loop)

    with (yield from pool) as conn:
        msg = yield from conn.echo('hello')
        assert msg == b'hello'
        assert pool.size == 10
        assert pool.freesize == 9
    assert pool.size == 10
    assert pool.freesize == 10


@pytest.mark.run_loop
def test_create_new(create_pool, loop, server):
    pool = yield from create_pool(
        server.tcp_address,
        minsize=1, loop=loop)
    assert pool.size == 1
    assert pool.freesize == 1

    with (yield from pool):
        assert pool.size == 1
        assert pool.freesize == 0

        with (yield from pool):
            assert pool.size == 2
            assert pool.freesize == 0

    assert pool.size == 2
    assert pool.freesize == 2


@pytest.mark.run_loop
def test_create_constraints(create_pool, loop, server):
    pool = yield from create_pool(
        server.tcp_address,
        minsize=1, maxsize=1, loop=loop)
    assert pool.size == 1
    assert pool.freesize == 1

    with (yield from pool):
        assert pool.size == 1
        assert pool.freesize == 0

        with pytest.raises(asyncio.TimeoutError):
            yield from asyncio.wait_for(pool.acquire(),
                                        timeout=0.2,
                                        loop=loop)


@pytest.mark.run_loop
def test_create_no_minsize(create_pool, loop, server):
    pool = yield from create_pool(
        server.tcp_address,
        minsize=0, maxsize=1, loop=loop)
    assert pool.size == 0
    assert pool.freesize == 0

    with (yield from pool):
        assert pool.size == 1
        assert pool.freesize == 0

        with pytest.raises(asyncio.TimeoutError):
            yield from asyncio.wait_for(pool.acquire(),
                                        timeout=0.2,
                                        loop=loop)
    assert pool.size == 1
    assert pool.freesize == 1


@pytest.mark.run_loop
def test_release_closed(create_pool, loop, server):
    pool = yield from create_pool(
        server.tcp_address,
        minsize=1, loop=loop)
    assert pool.size == 1
    assert pool.freesize == 1

    with (yield from pool) as redis:
        redis.close()
        yield from redis.wait_closed()
    assert pool.size == 0
    assert pool.freesize == 0


@pytest.mark.run_loop
def test_release_bad_connection(create_pool, create_redis, loop, server):
    pool = yield from create_pool(
        server.tcp_address,
        loop=loop)
    conn = yield from pool.acquire()
    other_conn = yield from create_redis(
        server.tcp_address,
        loop=loop)
    with pytest.raises(AssertionError):
        pool.release(other_conn)

    pool.release(conn)
    other_conn.close()
    yield from other_conn.wait_closed()


@pytest.mark.run_loop
def test_select_db(create_pool, loop, server):
    pool = yield from create_pool(
        server.tcp_address,
        loop=loop)

    yield from pool.select(1)
    with (yield from pool) as redis:
        assert redis.db == 1


@pytest.mark.run_loop
def test_change_db(create_pool, loop, server):
    pool = yield from create_pool(
        server.tcp_address,
        minsize=1, db=0,
        loop=loop)
    assert pool.size == 1
    assert pool.freesize == 1

    with (yield from pool) as redis:
        yield from redis.select(1)
    assert pool.size == 0
    assert pool.freesize == 0

    with (yield from pool) as redis:
        assert pool.size == 1
        assert pool.freesize == 0

        yield from pool.select(1)
        assert pool.db == 1
        assert pool.size == 1
        assert pool.freesize == 0
    assert pool.size == 0
    assert pool.freesize == 0
    assert pool.db == 1


@pytest.mark.run_loop
def test_change_db_errors(create_pool, loop, server):
    pool = yield from create_pool(
        server.tcp_address,
        minsize=1, db=0,
        loop=loop)

    with pytest.raises(TypeError):
        yield from pool.select(None)
    assert pool.db == 0

    with (yield from pool):
        pass
    assert pool.size == 1
    assert pool.freesize == 1

    with pytest.raises(TypeError):
        yield from pool.select(None)
    assert pool.db == 0
    with pytest.raises(ValueError):
        yield from pool.select(-1)
    assert pool.db == 0
    with pytest.raises(ReplyError):
        yield from pool.select(100000)
    assert pool.db == 0


@pytest.mark.run_loop
def test_select_and_create(create_pool, loop, server):
    # trying to model situation when select and acquire
    # called simultaneously
    # but acquire freezes on _wait_select and
    # then continues with propper db
    @asyncio.coroutine
    def test():
        pool = yield from create_pool(
            server.tcp_address,
            minsize=1, db=0,
            loop=loop)
        db = 0
        while True:
            db = (db + 1) & 1
            _, conn = yield from asyncio.gather(pool.select(db),
                                                pool.acquire(),
                                                loop=loop)
            assert pool.db == db
            pool.release(conn)
            if conn.db == db:
                break
    yield from asyncio.wait_for(test(), 3, loop=loop)


@pytest.mark.run_loop
def test_response_decoding(create_pool, loop, server):
    pool = yield from create_pool(
        server.tcp_address,
        encoding='utf-8', loop=loop)

    assert pool.encoding == 'utf-8'
    with (yield from pool) as redis:
        yield from redis.set('key', 'value')
    with (yield from pool) as redis:
        res = yield from redis.get('key')
        assert res == 'value'


@pytest.mark.run_loop
def test_hgetall_response_decoding(create_pool, loop, server):
    pool = yield from create_pool(
        server.tcp_address,
        encoding='utf-8', loop=loop)

    assert pool.encoding == 'utf-8'
    with (yield from pool) as redis:
        yield from redis.delete('key1')
        yield from redis.hmset('key1', 'foo', 'bar')
        yield from redis.hmset('key1', 'baz', 'zap')
    with (yield from pool) as redis:
        res = yield from redis.hgetall('key1')
        assert res == {'foo': 'bar', 'baz': 'zap'}


@pytest.mark.run_loop
def test_crappy_multiexec(create_pool, loop, server):
    pool = yield from create_pool(
        server.tcp_address,
        encoding='utf-8', loop=loop,
        minsize=1, maxsize=1)

    with (yield from pool) as redis:
        yield from redis.set('abc', 'def')
        yield from redis.connection.execute('multi')
        yield from redis.set('abc', 'fgh')
    assert redis.closed is True
    with (yield from pool) as redis:
        value = yield from redis.get('abc')
    assert value == 'def'


@pytest.mark.run_loop
def test_pool_size_growth(create_pool, server, loop):
    pool = yield from create_pool(
        server.tcp_address,
        loop=loop,
        minsize=1, maxsize=1)

    done = set()
    tasks = []

    @asyncio.coroutine
    def task1(i):
        with (yield from pool):
            assert pool.size <= pool.maxsize
            assert pool.freesize == 0
            yield from asyncio.sleep(0.2, loop=loop)
            done.add(i)

    @asyncio.coroutine
    def task2():
        with (yield from pool):
            assert pool.size <= pool.maxsize
            assert pool.freesize >= 0
            assert done == {0, 1}

    for _ in range(2):
        tasks.append(asyncio.async(task1(_), loop=loop))
    tasks.append(asyncio.async(task2(), loop=loop))
    yield from asyncio.gather(*tasks, loop=loop)


@pytest.mark.run_loop
def test_pool_with_closed_connections(create_pool, server, loop):
    pool = yield from create_pool(
        server.tcp_address,
        loop=loop,
        minsize=1, maxsize=2)
    assert 1 == pool.freesize
    conn1 = pool._pool[0]
    conn1.close()
    assert conn1.closed is True
    assert 1 == pool.freesize
    with (yield from pool) as conn2:
        assert conn2.closed is False
        assert conn1 is not conn2


@pytest.mark.run_loop
def test_pool_close(create_pool, server, loop):
    pool = yield from create_pool(
        server.tcp_address, loop=loop)

    assert pool.closed is False

    with (yield from pool) as redis:
        assert (yield from redis.ping()) == b'PONG'

    pool.close()
    yield from pool.wait_closed()
    assert pool.closed is True

    with pytest.raises(PoolClosedError):
        with (yield from pool) as redis:
            assert (yield from redis.ping()) == b'PONG'


@pytest.mark.run_loop
def test_pool_close__used(create_pool, server, loop):
    pool = yield from create_pool(
        server.tcp_address, loop=loop)

    assert pool.closed is False

    with (yield from pool) as redis:
        pool.close()
        yield from pool.wait_closed()
        assert pool.closed is True

        with pytest.raises(ConnectionClosedError):
            yield from redis.ping()


@pytest.mark.run_loop
@pytest.redis_version(2, 8, 0, reason="maxclients config setting")
def test_pool_check_closed_when_exception(create_pool, create_redis,
                                          start_server, loop):
    server = start_server('server-small')
    redis = yield from create_redis(server.tcp_address, loop=loop)
    yield from redis.config_set('maxclients', 2)

    with pytest.logs('aioredis', 'DEBUG') as cm:
        with pytest.raises(Exception):
            yield from create_pool(address=tuple(server.tcp_address),
                                   minsize=2, loop=loop)

    assert len(cm.output) >= 3
    connect_msg = (
        "DEBUG:aioredis:Creating tcp connection"
        " to ('localhost', {})".format(server.tcp_address.port))
    assert cm.output[:2] == [connect_msg, connect_msg]
    assert cm.output[-1] == "DEBUG:aioredis:Closed 1 connections"
