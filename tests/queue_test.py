import uuid
import time
import asyncio
import unittest
from textwrap import dedent
from ._testutil import RedisTest, run_until_complete
from aioredis import RedisPool, ReplyError
from aioredis.util import Queue


class QueueTest(RedisTest):

    @run_until_complete
    def test_put_get(self):
        queue = Queue(connection=self.redis)
        self.assertTrue((yield from queue.empty()))
        self.assertFalse((yield from queue.get_nowait()))
        data = uuid.uuid4().bytes
        data2 = uuid.uuid4().bytes
        self.assertTrue((yield from queue.put(data)))
        self.assertEqual((yield from queue.qsize()), 1)
        self.assertTrue((yield from queue.put(data2)))
        self.assertEqual((yield from queue.qsize()), 2)
        self.assertEqual((yield from queue.get_nowait()), data)
        self.assertEqual((yield from queue.get_nowait()), data2)
        self.assertTrue((yield from queue.empty()))

    @run_until_complete
    def test_queue_size(self):
        queue = Queue(connection=self.redis, max_size=3)
        self.assertTrue((yield from queue.empty()))
        self.assertFalse((yield from queue.get_nowait()))
        self.assertTrue((yield from queue.put(uuid.uuid4().bytes)))
        self.assertTrue((yield from queue.put(uuid.uuid4().bytes)))
        self.assertFalse((yield from queue.full()))
        self.assertTrue((yield from queue.put(uuid.uuid4().bytes)))
        self.assertEqual((yield from queue.qsize()), 3)
        self.assertTrue((yield from queue.full()))
        self.assertFalse((yield from queue.put(uuid.uuid4().bytes)))
        self.assertEqual((yield from queue.qsize()), 3)
        self.assertTrue((yield from queue.full()))

    @run_until_complete
    def test_blocking_timeout(self):
        timeout = 1
        queue = Queue(connection=self.redis, timeout=timeout)
        query_start = time.time()
        self.assertTrue((yield from queue.empty()))
        # test default timeout
        data = yield from queue.get()
        self.assertFalse(data)
        self.assertGreaterEqual(time.time() - query_start, timeout)
        # test explicit timeout
        query_start = time.time()
        data = yield from queue.get(timeout=timeout)
        self.assertFalse(data)
        self.assertGreaterEqual(time.time() - query_start, timeout)

