import asyncio
import unittest
from unittest import mock

from aioredis.commands import MultiExec


class MultiExecTest(unittest.TestCase):

    @unittest.expectedFailure
    def test_global_loop(self):
        conn = mock.Mock()
        loop = asyncio.get_event_loop()
        tr = MultiExec(conn)
        self.assertIs(tr._loop, loop)

        def make_fut(cmd, *args):
            fut = asyncio.Future()
            if cmd == 'PING':
                fut.set_result(b'QUEUED')
            elif cmd == 'EXEC':
                fut.set_result(['PONG'])
            else:
                fut.set_result(b'OK')
            return fut

        conn.execute.side_effect = make_fut
        conn.closed = False
        conn._transaction_error = None

        @asyncio.coroutine
        def go():
            # FIXME: tr.connection -- will return fake connection
            #                         wrapper without execute method
            tr.connection.execute('PING')
            res = yield from tr.execute()
            self.assertEqual(res, [b'PONG'])
        loop.run_until_complete(go())
