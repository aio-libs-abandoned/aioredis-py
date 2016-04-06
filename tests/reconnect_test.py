from ._testutil import BaseTest, run_until_complete
from aioredis import create_reconnecting_redis


class ReconnectTest(BaseTest):

    @run_until_complete
    def test_recon(self):
        redis = yield from create_reconnecting_redis(
            ('localhost', self.redis_port), loop=self.loop)
        self.assertEqual(repr(redis), '<Redis <AutoConnector None>>')
        resp = yield from redis.echo('ECHO')
        self.assertEqual(resp, b'ECHO')
        self.assertEqual(repr(redis),
                         '<Redis <AutoConnector <RedisConnection [db:0]>>>')
        conn_id = id(redis._conn._conn)

        redis._conn._conn._do_close(ValueError("Emulate connection close"))

        resp = yield from redis.echo('ECHO')
        self.assertEqual(resp, b'ECHO')
        self.assertNotEqual(conn_id, id(redis._conn._conn))
        # FIXME: bad interface
        conn = yield from redis.connection.get_atomic_connection()
        conn.close()
        yield from conn.wait_closed()

    @run_until_complete
    def test_multi_exec(self):
        redis = yield from create_reconnecting_redis(
            ('localhost', self.redis_port), loop=self.loop)
        self.assertEqual(repr(redis), '<Redis <AutoConnector None>>')

        m = redis.multi_exec()
        m.echo('ECHO')
        res = yield from m.execute()
        self.assertEqual(res, [b'ECHO'])
        # FIXME: bad interface
        conn = yield from redis.connection.get_atomic_connection()
        conn.close()
        yield from conn.wait_closed()
