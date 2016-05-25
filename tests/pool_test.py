import asyncio
import pytest

from aioredis import RedisPool, ReplyError


def _assert_defaults(pool):
    assert isinstance(pool, RedisPool)
    assert pool.minsize == 1
    assert pool.maxsize == 10
    assert pool.size == 1
    assert pool.freesize == 1


def test_connect(create_pool, loop, server):
    pool = loop.run_until_complete(create_pool(
        ('localhost', server.port), loop=loop))
    _assert_defaults(pool)


def test_global_loop(create_pool, loop, server):
    asyncio.set_event_loop(loop)

    pool = loop.run_until_complete(create_pool(
        ('localhost', server.port)))
    _assert_defaults(pool)


@pytest.mark.run_loop
def test_clear(create_pool, loop, server):
    pool = yield from create_pool(
        ('localhost', server.port), loop=loop)
    _assert_defaults(pool)

    yield from pool.clear()
    assert pool.freesize == 0


@pytest.mark.run_loop
def test_no_yield_from(create_pool, loop, server):
    pool = yield from create_pool(
        ('localhost', server.port), loop=loop)

    with pytest.raises(RuntimeError):
        with pool:
            pass


@pytest.mark.run_loop
def test_simple_command(create_pool, loop, server):
    pool = yield from create_pool(
        ('localhost', server.port),
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
        ('localhost', server.port),
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
        ('localhost', server.port),
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
        ('localhost', server.port),
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
        ('localhost', server.port),
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
        ('localhost', server.port),
        loop=loop)
    conn = yield from pool.acquire()
    other_conn = yield from create_redis(
        ('localhost', server.port),
        loop=loop)
    with pytest.raises(AssertionError):
        pool.release(other_conn)

    pool.release(conn)
    other_conn.close()
    yield from other_conn.wait_closed()


@pytest.mark.run_loop
def test_select_db(create_pool, loop, server):
    pool = yield from create_pool(
        ('localhost', server.port),
        loop=loop)

    yield from pool.select(1)
    with (yield from pool) as redis:
        assert redis.db == 1


@pytest.mark.run_loop
def test_change_db(create_pool, loop, server):
    pool = yield from create_pool(
        ('localhost', server.port),
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
        ('localhost', server.port),
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
            ('localhost', server.port),
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
    yield from asyncio.wait_for(test(), 1, loop=loop)


@pytest.mark.run_loop
def test_response_decoding(create_pool, loop, server):
    pool = yield from create_pool(
        ('localhost', server.port),
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
        ('localhost', server.port),
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
        ('localhost', server.port),
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
        ('localhost', server.port),
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
        ('localhost', server.port),
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
