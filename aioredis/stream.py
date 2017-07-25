import asyncio


class StreamReader(asyncio.StreamReader):
    """
    Override the official StreamReader to address the
    following issue: http://bugs.python.org/issue30861

    Also it leverages to get rid of the dobule buffer and
    get rid of one coroutine step. Data flows from the buffer
    to the Redis parser directly.
    """

    def set_parser(self, parser):
        self._parser = parser

    def feed_data(self, data):
        assert not self._eof, 'feed_data after feed_eof'

        if not data:
            return

        self._parser.feed(data)
        self._wakeup_waiter()

        # TODO: implement pause the read. Its needed
        #       expose the len of the buffer from hiredis
        #       to make it possible.

    @asyncio.coroutine
    def readobj(self):
        """
        Return a parsed Redis object or an exception
        when something wrong happened.
        """
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

            yield from self._wait_for_data('readobj')

    @asyncio.coroutine
    def _read_not_allowed(self, *args, **kwargs):
        raise RuntimeError('Use readobj')

    read = _read_not_allowed
    readline = _read_not_allowed
    readuntil = _read_not_allowed
    readexactly = _read_not_allowed
