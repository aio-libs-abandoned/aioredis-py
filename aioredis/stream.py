import asyncio

__all__ = [
    'open_connection',
    'open_unix_connection',
    'StreamReader',
]


async def open_connection(host=None, port=None, *,
                          limit, loop=None,
                          parser=None, **kwds):
    # XXX: parser is not used (yet)
    if loop is None:
        loop = asyncio.get_event_loop()
    reader = StreamReader(limit=limit, loop=loop)
    protocol = asyncio.StreamReaderProtocol(reader, loop=loop)
    transport, _ = await loop.create_connection(
        lambda: protocol, host, port, **kwds)
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)
    return reader, writer


async def open_unix_connection(address, *,
                               limit, loop=None,
                               parser=None, **kwds):
    # XXX: parser is not used (yet)
    if loop is None:
        loop = asyncio.get_event_loop()
    reader = StreamReader(limit=limit, loop=loop)
    protocol = asyncio.StreamReaderProtocol(reader, loop=loop)
    transport, _ = await loop.create_unix_connection(
        lambda: protocol, address, **kwds)
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)
    return reader, writer


class StreamReader(asyncio.StreamReader):
    """
    Override the official StreamReader to address the
    following issue: http://bugs.python.org/issue30861

    Also it leverages to get rid of the dobule buffer and
    get rid of one coroutine step. Data flows from the buffer
    to the Redis parser directly.
    """
    _parser = None

    def set_parser(self, parser):
        self._parser = parser
        if self._buffer:
            self._parser.feed(self._buffer)
            del self._buffer[:]

    def feed_data(self, data):
        assert not self._eof, 'feed_data after feed_eof'

        if not data:
            return
        if self._parser is None:
            # XXX: hopefully it's only a small error message
            self._buffer.extend(data)
            return
        self._parser.feed(data)
        self._wakeup_waiter()

        # TODO: implement pause the read. Its needed
        #       expose the len of the buffer from hiredis
        #       to make it possible.

    async def readobj(self):
        """
        Return a parsed Redis object or an exception
        when something wrong happened.
        """
        assert self._parser is not None, "set_parser must be called"
        while True:
            obj = self._parser.gets()

            if obj is not False:
                # TODO: implement resume the read

                # Return any valid object and the Nil->None
                # case. When its False there is nothing there
                # to be parsed and we have to wait for more data.
                return obj

            if self._exception:
                raise self._exception

            if self._eof:
                break

            await self._wait_for_data('readobj')
        # NOTE: after break we return None which must be handled as b''

    async def _read_not_allowed(self, *args, **kwargs):
        raise RuntimeError('Use readobj')

    read = _read_not_allowed
    readline = _read_not_allowed
    readuntil = _read_not_allowed
    readexactly = _read_not_allowed
