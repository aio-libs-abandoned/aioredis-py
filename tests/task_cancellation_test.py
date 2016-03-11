import asyncio

from ._testutil import BaseTest, run_until_complete


class TaskCancellationTest(BaseTest):

    @run_until_complete
    def test_future_cancellation(self):
        conn = yield from self.create_connection(
            ('localhost', 6379), loop=self.loop)

        ts = self.loop.time()
        fut = conn.execute('BLPOP', 'some-list', 5)
        with self.assertRaises(asyncio.TimeoutError):
            yield from asyncio.wait_for(fut, 1, loop=self.loop)
        self.assertTrue(fut.cancelled())

        # XXX: Connection becomes available only after timeout expires
        yield from conn.execute('TIME')
        dt = self.loop.time() - ts
        self.assertAlmostEqual(dt, 5.0, delta=1)
