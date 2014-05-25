import asyncio
import unittest
import socket
import random

from functools import wraps


def test_coroutine(fun):
    if not asyncio.iscoroutinefunction(fun):
        fun = asyncio.coroutine(fun)

    @wraps(fun)
    def wrapper(test, *args, **kw):
        loop = test.loop
        ret = loop.run_until_complete(fun(test, *args, **kw))
        return ret
    return wrapper


class BaseTest(unittest.TestCase):
    """Base test case for unittests.
    """

    def setUp(self):
        self.loop = loop = asyncio.new_event_loop()
        self.redis_port = self._find_port()
        self.redis = loop.run_until_complete(asyncio.create_subprocess_exec(
            'redis-server',
            '--bind', 'localhost',
            '--port', str(self.redis_port),
            '--unixsocket', '/tmp/aioredis.sock',
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            loop=loop))
        self.loop.run_until_complete(asyncio.sleep(0.01, loop=self.loop))

    def tearDown(self):
        if self.redis is not None:
            self.redis.terminate()
            # self.loop.run_until_complete(self.redis.wait())
        self.loop.close()
        del self.loop

    def _find_port(self):
        s = socket.socket()
        while True:
            try:
                s.bind(('127.0.0.1', 0))
            except OSError:
                pass
            else:
                addr = s.getsockname()
                s.close()
                return addr[1]
