import asyncio

from ._testutil import RedisTest, run_until_complete


class PubSubCommandsTest(RedisTest):

    @asyncio.coroutine
    def _reader(self, channel, output, waiter, conn=None):
        if conn is None:
            conn = yield from self.create_connection(
                ('localhost', 6379), loop=self.loop)
        yield from conn.execute('subscribe', channel)
        ch = conn.pubsub_channels[channel]
        waiter.set_result(conn)
        while (yield from ch.wait_message()):
            msg = yield from ch.get()
            yield from output.put(msg)

    @run_until_complete
    def test_publish(self):
        out = asyncio.Queue(loop=self.loop)
        fut = asyncio.Future(loop=self.loop)
        sub = asyncio.async(self._reader('chan:1', out, fut),
                            loop=self.loop)

        redis = yield from self.create_redis(
            ('localhost', 6379), loop=self.loop)

        yield from fut
        yield from redis.publish('chan:1', 'Hello')
        msg = yield from out.get()
        self.assertEqual(msg, b'Hello')

        sub.cancel()

    @run_until_complete
    def test_publish_json(self):
        out = asyncio.Queue(loop=self.loop)
        fut = asyncio.Future(loop=self.loop)
        sub = asyncio.async(self._reader('chan:1', out, fut),
                            loop=self.loop)

        redis = yield from self.create_redis(
            ('localhost', 6379), loop=self.loop)
        yield from fut

        res = yield from redis.publish_json('chan:1', {"Hello": "world"})
        self.assertEqual(res, 1)    # recievers

        msg = yield from out.get()
        self.assertEqual(msg, b'{"Hello": "world"}')
        sub.cancel()
