import asyncio
import unittest
import socket
import random
import os

from functools import wraps


def run_until_complete(fun):
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
        self.loop = asyncio.new_event_loop()
        self.redis_port = int(os.environ.get('REDIS_PORT') or 6379)
        socket = os.environ.get('REDIS_SOCKET')
        self.redis_socket = socket or '/tmp/aioredis.sock'

    def tearDown(self):
        self.loop.close()
        del self.loop

    def _find_port(self):
        s = socket.socket()
        while True:
            port = random.randint(1024, 65535)
            try:
                s.bind(('127.0.0.1', port))
            except OSError:
                pass
            else:
                s.close()
                return port
