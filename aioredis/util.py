import asyncio
import json
import sys

from asyncio.base_events import BaseEventLoop

from .errors import ChannelClosedError
from .log import logger

PY_35 = sys.version_info >= (3, 5)

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


def decode(obj, encoding):
    if isinstance(obj, bytes):
        return obj.decode(encoding)
    elif isinstance(obj, list):
        return [decode(o, encoding) for o in obj]
    return obj


class Channel:
    """Wrapper around asyncio.Queue."""
    __slots__ = ('_queue', '_name',
                 '_closed', '_waiter',
                 '_is_pattern', '_loop')

    def __init__(self, name, is_pattern, loop=None):
        self._queue = asyncio.Queue(loop=loop)
        self._name = name
        self._is_pattern = is_pattern
        self._loop = loop
        self._closed = False
        self._waiter = None

    def __repr__(self):
        return "<Channel name:{}, is_pattern:{}, qsize:{}>".format(
            self._name, self._is_pattern, self._queue.qsize())

    @property
    def name(self):
        """Encoded channel name/pattern."""
        return self._name

    @property
    def is_pattern(self):
        """Set to True if channel is subscribed to pattern."""
        return self._is_pattern

    @property
    def is_active(self):
        """Returns True until there are messages in channel or
        connection is subscribed to it.

        Can be used with ``while``:

        >>> ch = conn.pubsub_channels['chan:1']
        >>> while ch.is_active:
        ...     msg = yield from ch.get()   # may stuck for a long time

        """
        return not (self._queue.qsize() <= 1 and self._closed)

    @asyncio.coroutine
    def get(self, *, encoding=None, decoder=None):
        """Coroutine that waits for and returns a message.

        Raises ChannelClosedError exception if channel is unsubscribed
        and has no messages.
        """
        assert decoder is None or callable(decoder), decoder
        if not self.is_active:
            if self._queue.qsize() == 1:
                msg = self._queue.get_nowait()
                assert msg is None, msg
                return
            raise ChannelClosedError()
        msg = yield from self._queue.get()
        if msg is None:
            # TODO: maybe we need an explicit marker for "end of stream"
            #       currently, returning None may overlap with
            #       possible return value from `decoder`
            #       so the user would have to check `ch.is_active`
            #       to determine if its EoS or payload
            return
        if self._is_pattern:
            dest_channel, msg = msg
        if encoding is not None:
            msg = msg.decode(encoding)
        if decoder is not None:
            msg = decoder(msg)
        if self._is_pattern:
            return dest_channel, msg
        return msg

    @asyncio.coroutine
    def get_json(self, encoding='utf-8'):
        """Shortcut to get JSON messages."""
        return (yield from self.get(encoding=encoding, decoder=json.loads))

    if PY_35:
        def iter(self, *, encoding=None, decoder=None):
            """Same as get method but its native coroutine.

            Usage example:

            >>> async for msg in ch.iter():
            ...     print(msg)
            """
            return _ChannelIter(self, encoding=encoding,
                                decoder=decoder)

    @asyncio.coroutine
    def wait_message(self):
        """Waits for message to become available in channel.

        Possible usage:

        >>> while (yield from ch.wait_message()):
        ...     msg = yield from ch.get()
        """
        if not self.is_active:
            return False
        if not self._queue.empty():
            return True
        if self._waiter is None:
            self._waiter = create_future(loop=self._loop)
        yield from self._waiter
        return self.is_active

    # internale methods

    def put_nowait(self, data):
        self._queue.put_nowait(data)
        if self._waiter is not None:
            fut, self._waiter = self._waiter, None
            if fut.done():
                assert fut.cancelled(), (
                    "Waiting future is in wrong state", self, fut)
                return
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
    if res in (b'QUEUED', 'QUEUED'):
        return res
    return res in (b'OK', 'OK')


@asyncio.coroutine
def wait_convert(fut, type_):
    result = yield from fut
    if result in (b'QUEUED', 'QUEUED'):
        return result
    return type_(result)


@asyncio.coroutine
def wait_make_dict(fut):
    res = yield from fut
    if res in (b'QUEUED', 'QUEUED'):
        return res
    it = iter(res)
    return dict(zip(it, it))


class coerced_keys_dict(dict):

    def __getitem__(self, other):
        if not isinstance(other, bytes):
            other = _converters[type(other)](other)
        return dict.__getitem__(self, other)

    def __contains__(self, other):
        if not isinstance(other, bytes):
            other = _converters[type(other)](other)
        return dict.__contains__(self, other)


if PY_35:
    class _BaseScanIter:
        __slots__ = ('_scan', '_cur', '_ret')

        def __init__(self, scan):
            self._scan = scan
            self._cur = b'0'
            self._ret = []

        @asyncio.coroutine
        def __aiter__(self):
            return self

    class _ScanIter(_BaseScanIter):

        @asyncio.coroutine
        def __anext__(self):
            while not self._ret and self._cur:
                self._cur, self._ret = yield from self._scan(self._cur)
            if not self._cur and not self._ret:
                raise StopAsyncIteration  # noqa
            else:
                ret = self._ret.pop(0)
                return ret

    class _ScanIterPairs(_BaseScanIter):

        @asyncio.coroutine
        def __anext__(self):
            while not self._ret and self._cur:
                self._cur, ret = yield from self._scan(self._cur)
                self._ret = list(zip(ret[::2], ret[1::2]))
            if not self._cur and not self._ret:
                raise StopAsyncIteration  # noqa
            else:
                ret = self._ret.pop(0)
                return ret

    class _ChannelIter:

        __slots__ = ('_ch', '_args', '_kw')

        def __init__(self, ch, *args, **kw):
            self._ch = ch
            self._args = args
            self._kw = kw

        @asyncio.coroutine
        def __aiter__(self):
            return self

        @asyncio.coroutine
        def __anext__(self):
            if not self._ch.is_active:
                raise StopAsyncIteration    # noqa
            msg = yield from self._ch.get(*self._args, **self._kw)
            if msg is None:
                raise StopAsyncIteration    # noqa
            return msg


def _set_result(fut, result):
    if fut.done():
        logger.debug("Waiter future is already done %r", fut)
        assert fut.cancelled(), (
            "waiting future is in wrong state", fut, result)
    else:
        fut.set_result(result)


def _set_exception(fut, exception):
    if fut.done():
        logger.debug("Waiter future is already done %r", fut)
        assert fut.cancelled(), (
            "waiting future is in wrong state", fut, exception)
    else:
        fut.set_exception(exception)


if hasattr(asyncio, 'ensure_future'):
    async_task = asyncio.ensure_future
else:
    async_task = asyncio.async  # Deprecated since 3.4.4


# create_future is new in version 3.5.2
if hasattr(BaseEventLoop, 'create_future'):
    def create_future(loop):
        return loop.create_future()
else:
    def create_future(loop):
        return asyncio.Future(loop=loop)
