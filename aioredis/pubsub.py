import asyncio
import json
import sys
import weakref

from .util import create_future, _converters
from .errors import ChannelClosedError

__all__ = [
    "Channel",
    "EndOfStream",
    "Listener",
]

PY_35 = sys.version_info >= (3, 5)


# End of pubsub messages stream marker.
EndOfStream = object()


class Channel:
    """Wrapper around asyncio.Queue."""
    __slots__ = ('_queue', '_name',
                 '_closed', '_waiter',
                 '_is_pattern', '_loop')

    def __init__(self, name, is_pattern, loop=None):
        self._queue = asyncio.Queue(loop=loop)
        self._name = _converters[type(name)](name)
        self._is_pattern = is_pattern
        self._loop = loop
        self._closed = False
        self._waiter = None

    def __repr__(self):
        return "<{} name:{!r}, is_pattern:{}, qsize:{}>".format(
            self.__class__.__name__,
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


if PY_35:
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


class Listener:
    """Multi-producers, single-consumer Pub/Sub queue.

    Can be used in cases where a single consumer task
    must read messages from several different channels
    (where pattern subscriptions may not work well).

    Example use case:

    >>> mpsc = Listener(loop=loop)
    >>> async def reader(mpsc):
    ...     async for channel, msg in mpsc.iter():
    ...         assert isinstance(channel, Channel)
    ...         print("Got {!r} in channel {!r}".format(msg, channel))
    >>> asyncio.ensure_future(reader(mpsc))
    >>> redis.subscribe(mpsc.channel('channel:1'),
    ...                 mpsc.channel('channel:3'))
    ...                 mpsc.channel('channel:5'))
    >>> redis.psubscribe(mpsc.pattern('hello'))
    >>> # publishing 'Hello world' into 'hello-channel'
    >>> # will print this message:
    Got b'Hello world' in channel b'hello-channel'
    """

    __slots__ = ('_queue', '_refs', '_loop')

    def __init__(self, maxsize=None, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self._queue = asyncio.Queue(loop=loop)
        self._refs = weakref.WeakSet()
        self._loop = loop

    def channel(self, name):
        """Create channel."""
        # TODO keep dict of channels:
        # * to be able to tell if listener still active.
        # * maybe this should be controlled in some other way,
        #       eg listener will close all channels?
        #       channels can be disconnected elsewhere
        #       so the listener may stuck waiting for closed channels
        #   we can store weakrefs to channels so we can
        #   test if channel is still subscribed
        ch = _Sender(self._queue, name,
                     is_pattern=False,
                     loop=self._loop)
        self._refs.add(ch)
        return ch

    def pattern(self, pattern):
        """Create pattern channel."""
        ch = _Sender(self._queue, pattern,
                     is_pattern=True,
                     loop=self._loop)
        self._refs.add(ch)
        return ch

    @asyncio.coroutine
    def get(self, *, encoding=None, decoder=None):
        raise NotImplementedError

    @asyncio.coroutine
    def wait_message(self):
        raise NotImplementedError

    @property
    def is_active(self):
        """Returns True if listener has any active subscription."""
        # TODO: check queue is not empty
        return (len(self._refs) and
                any(ch.is_active for ch in self._refs))


class _Sender(Channel):
    """Write-Only Channel.

    Does not allow direct `.get()` calls.
    """
    __slots__ = ('__weakref__',)

    def __init__(self, queue, name, is_pattern, *, loop):
        self._queue = queue
        self._name = _converters[type(name)](name)
        self._is_pattern = is_pattern
        self._loop = loop
        self._closed = False
        self._waiter = None

    @asyncio.coroutine
    def get(self, *, encoding=None, decoder=None):
        # TODO: impossible to use `get`
        #       'cause there can only be single consumer
        #       raise exception;
        raise RuntimeError("MPSC channel does not allow direct get")

    def put_nowait(self, data):
        super().put_nowait((self, data))
