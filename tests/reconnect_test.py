from ._testutil import BaseTest, run_until_complete
from aioredis import create_reconnecting_redis


class ReconnectTest(BaseTest):

    @run_until_complete
    def test_recon(self):
        redis = yield from create_reconnecting_redis(
            ('localhost', self.redis_port), db=1, loop=self.loop)
        self.assertEqual(repr(redis), '<Redis <AutoConnector None>>')
        resp = yield from redis.echo('ECHO')
        self.assertEqual(resp, b'ECHO')
        self.assertEqual(repr(redis),
                         '<Redis <AutoConnector <RedisConnection [db:1]>>>')
        conn_id = id(redis._conn._conn)

        redis._conn._conn._do_close(ValueError("Emulate connection close"))

        resp = yield from redis.echo('ECHO')
        self.assertEqual(resp, b'ECHO')
        self.assertNotEqual(conn_id, id(redis._conn._conn))
