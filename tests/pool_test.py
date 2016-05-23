import asyncio
import sys
import unittest

from textwrap import dedent
from ._testutil import BaseTest, run_until_complete
from aioredis import RedisPool, ReplyError

PY_35 = sys.version_info >= (3, 5)


class PoolTest(BaseTest):

    def _assert_defaults(self, pool):
        self.assertIsInstance(pool, RedisPool)
        self.assertEqual(pool.minsize, 1)
        self.assertEqual(pool.maxsize, 10)
        self.assertEqual(pool.size, 1)
        self.assertEqual(pool.freesize, 1)

    def test_connect(self):
        pool = self.loop.run_until_complete(self.create_pool(
            ('localhost', self.redis_port), loop=self.loop))
        self._assert_defaults(pool)

    def test_global_loop(self):
        asyncio.set_event_loop(self.loop)

        pool = self.loop.run_until_complete(self.create_pool(
            ('localhost', self.redis_port)))
        self._assert_defaults(pool)

    @run_until_complete
    def test_clear(self):
        pool = yield from self.create_pool(
            ('localhost', self.redis_port), loop=self.loop)
        self._assert_defaults(pool)

        yield from pool.clear()
        self.assertEqual(pool.freesize, 0)

    @run_until_complete
    def test_no_yield_from(self):
        pool = yield from self.create_pool(
            ('localhost', self.redis_port), loop=self.loop)

        with self.assertRaises(RuntimeError):
            with pool:
                pass

    @run_until_complete
    def test_simple_command(self):
        pool = yield from self.create_pool(
            ('localhost', self.redis_port),
            minsize=10, loop=self.loop)

        with (yield from pool) as conn:
            msg = yield from conn.echo('hello')
            self.assertEqual(msg, b'hello')
            self.assertEqual(pool.size, 10)
            self.assertEqual(pool.freesize, 9)
        self.assertEqual(pool.size, 10)
        self.assertEqual(pool.freesize, 10)

    @run_until_complete
    def test_create_new(self):
        pool = yield from self.create_pool(
            ('localhost', self.redis_port),
            minsize=1, loop=self.loop)
        self.assertEqual(pool.size, 1)
        self.assertEqual(pool.freesize, 1)

        with (yield from pool):
            self.assertEqual(pool.size, 1)
            self.assertEqual(pool.freesize, 0)

            with (yield from pool):
                self.assertEqual(pool.size, 2)
                self.assertEqual(pool.freesize, 0)

        self.assertEqual(pool.size, 2)
        self.assertEqual(pool.freesize, 2)

    @run_until_complete
    def test_create_constraints(self):
        pool = yield from self.create_pool(
            ('localhost', self.redis_port),
            minsize=1, maxsize=1, loop=self.loop)
        self.assertEqual(pool.size, 1)
        self.assertEqual(pool.freesize, 1)

        with (yield from pool):
            self.assertEqual(pool.size, 1)
            self.assertEqual(pool.freesize, 0)

            with self.assertRaises(asyncio.TimeoutError):
                yield from asyncio.wait_for(pool.acquire(),
                                            timeout=0.2,
                                            loop=self.loop)

    @run_until_complete
    def test_create_no_minsize(self):
        pool = yield from self.create_pool(
            ('localhost', self.redis_port),
            minsize=0, maxsize=1, loop=self.loop)
        self.assertEqual(pool.size, 0)
        self.assertEqual(pool.freesize, 0)

        with (yield from pool):
            self.assertEqual(pool.size, 1)
            self.assertEqual(pool.freesize, 0)

            with self.assertRaises(asyncio.TimeoutError):
                yield from asyncio.wait_for(pool.acquire(),
                                            timeout=0.2,
                                            loop=self.loop)
        self.assertEqual(pool.size, 1)
        self.assertEqual(pool.freesize, 1)

    @run_until_complete
    def test_release_closed(self):
        pool = yield from self.create_pool(
            ('localhost', self.redis_port),
            minsize=1, loop=self.loop)
        self.assertEqual(pool.size, 1)
        self.assertEqual(pool.freesize, 1)

        with (yield from pool) as redis:
            redis.close()
            yield from redis.wait_closed()
        self.assertEqual(pool.size, 0)
        self.assertEqual(pool.freesize, 0)

    @run_until_complete
    def test_release_bad_connection(self):
        pool = yield from self.create_pool(
            ('localhost', self.redis_port),
            loop=self.loop)
        conn = yield from pool.acquire()
        other_conn = yield from self.create_redis(
            ('localhost', self.redis_port),
            loop=self.loop)
        with self.assertRaises(AssertionError):
            pool.release(other_conn)

        pool.release(conn)
        other_conn.close()
        yield from other_conn.wait_closed()

    @run_until_complete
    def test_select_db(self):
        pool = yield from self.create_pool(
            ('localhost', self.redis_port),
            loop=self.loop)

        yield from pool.select(1)
        with (yield from pool) as redis:
            self.assertEqual(redis.db, 1)

    @run_until_complete
    def test_change_db(self):
        pool = yield from self.create_pool(
            ('localhost', self.redis_port),
            minsize=1, db=0,
            loop=self.loop)
        self.assertEqual(pool.size, 1)
        self.assertEqual(pool.freesize, 1)

        with (yield from pool) as redis:
            yield from redis.select(1)
        self.assertEqual(pool.size, 0)
        self.assertEqual(pool.freesize, 0)

        with (yield from pool) as redis:
            self.assertEqual(pool.size, 1)
            self.assertEqual(pool.freesize, 0)

            yield from pool.select(1)
            self.assertEqual(pool.db, 1)
            self.assertEqual(pool.size, 1)
            self.assertEqual(pool.freesize, 0)
        self.assertEqual(pool.size, 0)
        self.assertEqual(pool.freesize, 0)
        self.assertEqual(pool.db, 1)

    @run_until_complete
    def test_change_db_errors(self):
        pool = yield from self.create_pool(
            ('localhost', self.redis_port),
            minsize=1, db=0,
            loop=self.loop)

        with self.assertRaises(TypeError):
            yield from pool.select(None)
        self.assertEqual(pool.db, 0)

        with (yield from pool):
            pass
        self.assertEqual(pool.size, 1)
        self.assertEqual(pool.freesize, 1)

        with self.assertRaises(TypeError):
            yield from pool.select(None)
        self.assertEqual(pool.db, 0)
        with self.assertRaises(ValueError):
            yield from pool.select(-1)
        self.assertEqual(pool.db, 0)
        with self.assertRaises(ReplyError):
            yield from pool.select(100000)
        self.assertEqual(pool.db, 0)

    @run_until_complete
    def test_select_and_create(self):
        # trying to model situation when select and acquire
        # called simultaneously
        # but acquire freezes on _wait_select and
        # then continues with propper db
        @asyncio.coroutine
        def test():
            pool = yield from self.create_pool(
                ('localhost', self.redis_port),
                minsize=1, db=0,
                loop=self.loop)
            db = 0
            while True:
                db = (db + 1) & 1
                _, conn = yield from asyncio.gather(pool.select(db),
                                                    pool.acquire(),
                                                    loop=self.loop)
                self.assertEqual(pool.db, db)
                pool.release(conn)
                if conn.db == db:
                    break
        yield from asyncio.wait_for(test(), 1, loop=self.loop)

    @run_until_complete
    def test_response_decoding(self):
        pool = yield from self.create_pool(
            ('localhost', self.redis_port),
            encoding='utf-8', loop=self.loop)

        self.assertEqual(pool.encoding, 'utf-8')
        with (yield from pool) as redis:
            yield from redis.set('key', 'value')
        with (yield from pool) as redis:
            res = yield from redis.get('key')
            self.assertEqual(res, 'value')

    @run_until_complete
    def test_hgetall_response_decoding(self):
        pool = yield from self.create_pool(
            ('localhost', self.redis_port),
            encoding='utf-8', loop=self.loop)

        self.assertEqual(pool.encoding, 'utf-8')
        with (yield from pool) as redis:
            yield from redis.delete('key1')
            yield from redis.hmset('key1', 'foo', 'bar')
            yield from redis.hmset('key1', 'baz', 'zap')
        with (yield from pool) as redis:
            res = yield from redis.hgetall('key1')
            self.assertEqual(res, {'foo': 'bar', 'baz': 'zap'})

    @run_until_complete
    def test_crappy_multiexec(self):
        pool = yield from self.create_pool(
            ('localhost', self.redis_port),
            encoding='utf-8', loop=self.loop,
            minsize=1, maxsize=1)

        with (yield from pool) as redis:
            yield from redis.set('abc', 'def')
            yield from redis.connection.execute('multi')
            yield from redis.set('abc', 'fgh')
        self.assertTrue(redis.closed)
        with (yield from pool) as redis:
            value = yield from redis.get('abc')
        self.assertEquals(value, 'def')

    # @unittest.expectedFailure
    @run_until_complete
    def test_pool_size_growth(self):
        pool = yield from self.create_pool(
            ('localhost', self.redis_port),
            loop=self.loop,
            minsize=1, maxsize=1)

        done = set()
        tasks = []

        @asyncio.coroutine
        def task1(i):
            with (yield from pool):
                self.assertLessEqual(pool.size, pool.maxsize)
                self.assertEqual(pool.freesize, 0)
                yield from asyncio.sleep(0.2, loop=self.loop)
                done.add(i)

        @asyncio.coroutine
        def task2():
            with (yield from pool):
                self.assertLessEqual(pool.size, pool.maxsize)
                self.assertGreaterEqual(pool.freesize, 0)
                self.assertEqual(done, {0, 1})

        for _ in range(2):
            tasks.append(asyncio.async(task1(_), loop=self.loop))
        tasks.append(asyncio.async(task2(), loop=self.loop))
        yield from asyncio.gather(*tasks, loop=self.loop)

    @run_until_complete
    def test_pool_with_closed_connections(self):
        pool = yield from self.create_pool(
            ('localhost', self.redis_port),
            loop=self.loop,
            minsize=1, maxsize=2)
        self.assertEqual(1, pool.freesize)
        conn1 = pool._pool[0]
        conn1.close()
        self.assertTrue(conn1.closed)
        self.assertEqual(1, pool.freesize)
        with (yield from pool) as conn2:
            self.assertFalse(conn2.closed)
            self.assertIsNot(conn1, conn2)

    @unittest.skipUnless(PY_35, "Python 3.5+ required")
    @run_until_complete
    def test_await(self):
        pool = yield from self.create_pool(
            ('localhost', self.redis_port),
            minsize=10, loop=self.loop)

        s = dedent('''\
        async def coro(testcase, pool):
            with await pool as conn:
                msg = await conn.echo('hello')
                testcase.assertEqual(msg, b'hello')
        ''')
        exec(s, globals(), locals())
        yield from locals()['coro'](self, pool)

    @unittest.skipUnless(PY_35, "Python 3.5+ required")
    @run_until_complete
    def test_async_with(self):
        pool = yield from self.create_pool(
            ('localhost', self.redis_port),
            minsize=10, loop=self.loop)

        s = dedent('''\
        async def coro(testcase, pool):
            async with pool.get() as conn:
                msg = await conn.echo('hello')
                testcase.assertEqual(msg, b'hello')
        ''')
        exec(s, globals(), locals())
        yield from locals()['coro'](self, pool)
