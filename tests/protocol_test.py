import asyncio

from aioredis._testutil import BaseTest, test_coroutine

from aioredis import RedisProtocol


class ProtocolTest(BaseTest):

    @test_coroutine
    @asyncio.coroutine
    def test_create_connection(self):
        tr, proto = yield from self.loop.create_connection(
            lambda: RedisProtocol(loop=self.loop),
            '127.0.0.1', self.redis_port)
        proto.transport.close()
        return
        yield
