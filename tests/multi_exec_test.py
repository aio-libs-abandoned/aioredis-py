import asyncio
import unittest
from unittest import mock

from aioredis.commands import MultiExec
from aioredis.commands import Redis


class MultiExecTest(unittest.TestCase):

    def test_global_loop(self):
        conn = mock.Mock()
        # Needs to return yourself in get_atomic_connection()
        conn.get_atomic_connection.side_effect = \
            asyncio.coroutine(lambda: conn)
        try:
            old_loop = asyncio.get_event_loop()
        except (AssertionError, RuntimeError):
            old_loop = None
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        tr = MultiExec(conn, commands_factory=Redis)
        self.assertIs(tr._loop, loop)

        def make_fut(cmd, *args, **kw):
            fut = asyncio.Future()
            if cmd == 'PING':
                fut.set_result(b'QUEUED')
            elif cmd == 'EXEC':
                fut.set_result([b'PONG'])
            else:
                fut.set_result(b'OK')
            return fut

        conn.execute.side_effect = make_fut
        conn.closed = False
        conn._transaction_error = None

        @asyncio.coroutine
        def go():
            tr.ping()
            res = yield from tr.execute()
            self.assertEqual(res, [b'PONG'])
        loop.run_until_complete(go())
        asyncio.set_event_loop(old_loop)
