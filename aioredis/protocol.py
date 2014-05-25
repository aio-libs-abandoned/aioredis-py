import asyncio
# TODO: implement pure-python parser
import hiredis

from asyncio import protocols
from collections import deque


class RedisProtocol(protocols.Protocol):
    """Redis protocol implementation.
    """

    def __init__(self, loop=None):
        self._loop = loop
        self._waiters = deque()
        self._parser = hiredis.Reader()
        self.transport = None

    def connection_made(self, transport):
        """Called when a connection is made.

        The argument is the transport representing the pipe connection.
        To receive data, wait for data_received() calls.
        When the connection is closed, connection_lost() is called.
        """
        self.transport = transport

    def connection_lost(self, exc):
        """Called when the connection is lost or closed.

        The argument is an exception object or None (the latter
        meaning a regular EOF is received or the connection was
        aborted or closed).
        """
        self.transport = None
        while self._waiters:
            waiter = self._waiters.popleft()
            if exc is None:
                waiter.cancel()
            else:
                waiter.set_exception(exc)

    def data_received(self, data):
        """Called when some data is received.

        The argument is a bytes object.
        """
        # NOTE: write better doc string
        assert self._waiters, "got data, but no waiters"

        self._parser.feed(data)
        while True:
            obj = self._parser.gets()
            if obj is False:
                break
            waiter = self._waiters.popleft()
            waiter.set_result(obj)

    def execute(self, cmd, *args):
        """Executes redis command and returns Future waiting for the answer.

        """
        assert isinstance(cmd, (str, bytes, bytearray)), cmd

        data = encode_command(cmd, *args)
        waiter = asyncio.Future(loop=self._loop)
        self.transport.write(data)
        self._waiters.append(waiter)
        return waiter


_converters = {
    bytes: lambda val: val,
    bytearray: lambda val: val,
    str: lambda val: val.encode('utf-8'),
    int: lambda val: str(val).encode('utf-8'),
    float: lambda val: str(val).encode('utf-8'),
    }


def encode_command(*args):
    """Encodes arguments into redis bulk-strings array.

    Raises TypeError if any of args not of bytes, str, int or float type.
    """
    buf = bytearray()
    add = buf.extend
    add(b'*')
    add(str(len(args)).encode('utf-8'))
    add(b'\r\n')
    for arg in args:
        if arg is None:
            add(b'$-1\r\n')
        elif type(arg) in _converters:
            barg = _converters[type(arg)](arg)
            add(b'$')
            add(str(len(barg)).encode('utf-8'))
            add(b'\r\n')
            add(barg)
            add(b'\r\n')
        else:
            raise TypeError("Argument {!r} expected to be of bytes,"
                            " str, int or float type".format(arg))
    return buf
