import asyncio
import json

_NOTSET = object()

# NOTE: never put here anything else;
#       just this basic types
_converters = {
    bytes: lambda val: val,
    bytearray: lambda val: val,
    str: lambda val: val.encode('utf-8'),
    int: lambda val: str(val).encode('utf-8'),
    float: lambda val: str(val).encode('utf-8'),
    }


def _bytes_len(sized):
    return str(len(sized)).encode('utf-8')


def encode_command(*args):
    """Encodes arguments into redis bulk-strings array.

    Raises TypeError if any of args not of bytes, str, int or float type.
    """
    buf = bytearray()

    def add(data):
        return buf.extend(data + b'\r\n')

    add(b'*' + _bytes_len(args))
    for arg in args:
        if type(arg) in _converters:
            barg = _converters[type(arg)](arg)
            add(b'$' + _bytes_len(barg))
            add(barg)
        else:
            raise TypeError("Argument {!r} expected to be of bytes,"
                            " str, int or float type".format(arg))
    return buf


class Channel:
    """Wrapper around asyncio.Queue."""
    __slots__ = ('_queue', '_name',
                 '_closed', '_waiter',
                 '_loop')

    def __init__(self, name, loop=None):
        self._queue = asyncio.Queue(loop=loop)
        self._name = name
        self._loop = loop
        self._closed = False
        self._waiter = None

    @property
    def name(self):
        """Encoded channel name/pattern."""
        return self._name

    @asyncio.coroutine
    def get(self, *, encoding=None, decoder=None):
        """Coroutine that waits for and returns a message.

        Raises (TBD) exception if channel is unsubscribed and has no messages.
        """
        assert decoder is None or callable(decoder), decoder
        if self._closed:
            pass    # raise error
        msg = yield from self._queue.get()
        if encoding is not None:
            msg = msg.decode(encoding)
        if decoder is not None:
            msg = decoder(msg)
        return msg

    @asyncio.coroutine
    def get_json(self, encoding='utf-8'):
        """Shortcut to get JSON messages."""
        return (yield from self.get(encoding=encoding, decoder=json.loads))

    def is_active(self):
        """Returns True until there are messages in channel or
        connection is subscribed to it.

        Can be used with ``while``:

        >>> ch = conn.pubsub_channels['chan:1']
        >>> while ch.is_active():
        ...     msg = yield from ch.get()   # may stuck for a long time

        """
        return not (self._queue.empty() and self._closed)

    @asyncio.coroutine
    def wait_message(self):
        """Waits for message to become available in channel.

        Possible usage:

        >>> while (yield from ch.wait_message()):
        ...     msg = yield from ch.get()
        """
        if not self.is_active():
            return False
        if self._waiter is None:
            self._waiter = asyncio.Future(loop=self._loop)
        yield from self._waiter
        return self.is_active()

    # internale methods

    def put_nowait(self, data):
        self._queue.put_nowait(data)
        if self._waiter is not None:
            fut, self._waiter = self._waiter, None
            fut.set_result(None)

    def close(self):
        """Marks channel as inactive.

        Internal method, will be called from connection
        on `unsubscribe` command.
        """
        if not self._closed:
            self.put_nowait(None)
        self._closed = True


@asyncio.coroutine
def wait_ok(fut):
    res = yield from fut
    if res == b'QUEUED':
        return res
    return res == b'OK'


@asyncio.coroutine
def wait_convert(fut, type_):
    result = yield from fut
    if result == b'QUEUED':
        return result
    return type_(result)


class coerced_keys_dict(dict):

    def __getitem__(self, other):
        if not isinstance(other, bytes):
            other = _converters[type(other)](other)
        return dict.__getitem__(self, other)

    def __contains__(self, other):
        if not isinstance(other, bytes):
            other = _converters[type(other)](other)
        return dict.__contains__(self, other)
