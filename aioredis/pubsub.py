import asyncio
import json
import types

from .abc import AbcChannel
from .util import _converters, _set_result
from .errors import ChannelClosedError
from .log import logger

__all__ = [
    "Channel",
    "EndOfStream",
    "Receiver",
]


# End of pubsub messages stream marker.
EndOfStream = object()


class Channel(AbcChannel):
    """Wrapper around asyncio.Queue."""
    # doesn't make much sense with inheritance
    # __slots__ = ('_queue', '_name',
    #              '_closed', '_waiter',
    #              '_is_pattern', '_loop')

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
        ...     msg = await ch.get()   # may stuck for a long time

        """
        return not (self._queue.qsize() <= 1 and self._closed)

    async def get(self, *, encoding=None, decoder=None):
        """Coroutine that waits for and returns a message.

        :raises aioredis.ChannelClosedError: If channel is unsubscribed
            and has no messages.
        """
        assert decoder is None or callable(decoder), decoder
        if not self.is_active:
            if self._queue.qsize() == 1:
                msg = self._queue.get_nowait()
                assert msg is None, msg
                return
            raise ChannelClosedError()
        msg = await self._queue.get()
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

    async def get_json(self, encoding='utf-8'):
        """Shortcut to get JSON messages."""
        return (await self.get(encoding=encoding, decoder=json.loads))

    def iter(self, *, encoding=None, decoder=None):
        """Same as get method but its native coroutine.

        Usage example:

        >>> async for msg in ch.iter():
        ...     print(msg)
        """
        return _IterHelper(self,
                           is_active=lambda ch: ch.is_active,
                           encoding=encoding,
                           decoder=decoder)

    async def wait_message(self):
        """Waits for message to become available in channel.

        Possible usage:

        >>> while (await ch.wait_message()):
        ...     msg = await ch.get()
        """
        if not self.is_active:
            return False
        if not self._queue.empty():
            return True
        if self._waiter is None:
            self._waiter = self._loop.create_future()
        await self._waiter
        return self.is_active

    # internal methods

    def put_nowait(self, data):
        self._queue.put_nowait(data)
        if self._waiter is not None:
            fut, self._waiter = self._waiter, None
            _set_result(fut, None, self)

    def close(self):
        """Marks channel as inactive.

        Internal method, will be called from connection
        on `unsubscribe` command.
        """
        if not self._closed:
            self.put_nowait(None)
        self._closed = True


class _IterHelper:

    __slots__ = ('_ch', '_is_active', '_args', '_kw')

    def __init__(self, ch, is_active, *args, **kw):
        self._ch = ch
        self._is_active = is_active
        self._args = args
        self._kw = kw

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._is_active(self._ch):
            raise StopAsyncIteration    # noqa
        msg = await self._ch.get(*self._args, **self._kw)
        if msg is None:
            raise StopAsyncIteration    # noqa
        return msg


class Receiver:
    """Multi-producers, single-consumer Pub/Sub queue.

    Can be used in cases where a single consumer task
    must read messages from several different channels
    (where pattern subscriptions may not work well
    or channels can be added/removed dynamically).

    Example use case:

    >>> from aioredis.pubsub import Receiver
    >>> from aioredis.abc import AbcChannel
    >>> mpsc = Receiver(loop=loop)
    >>> async def reader(mpsc):
    ...     async for channel, msg in mpsc.iter():
    ...         assert isinstance(channel, AbcChannel)
    ...         print("Got {!r} in channel {!r}".format(msg, channel))
    >>> asyncio.ensure_future(reader(mpsc))
    >>> await redis.subscribe(mpsc.channel('channel:1'),
    ...                       mpsc.channel('channel:3'))
    ...                       mpsc.channel('channel:5'))
    >>> await redis.psubscribe(mpsc.pattern('hello'))
    >>> # publishing 'Hello world' into 'hello-channel'
    >>> # will print this message:
    Got b'Hello world' in channel b'hello-channel'
    >>> # when all is done:
    >>> await redis.unsubscribe('channel:1', 'channel:3', 'channel:5')
    >>> await redis.punsubscribe('hello')
    >>> mpsc.stop()
    >>> # any message received after stop() will be ignored.
    """

    def __init__(self, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self._queue = asyncio.Queue(loop=loop)
        self._refs = {}
        self._waiter = None
        self._running = True
        self._loop = loop

    def __repr__(self):
        return ('<Receiver is_active:{}, senders:{}, qsize:{}>'
                .format(self.is_active, len(self._refs), self._queue.qsize()))

    def channel(self, name):
        """Create a channel.

        Returns ``_Sender`` object implementing
        :class:`~aioredis.abc.AbcChannel`.
        """
        enc_name = _converters[type(name)](name)
        if (enc_name, False) not in self._refs:
            ch = _Sender(self, enc_name,
                         is_pattern=False,
                         loop=self._loop)
            self._refs[(enc_name, False)] = ch
            return ch
        return self._refs[(enc_name, False)]

    def pattern(self, pattern):
        """Create a pattern channel.

        Returns ``_Sender`` object implementing
        :class:`~aioredis.abc.AbcChannel`.
        """
        enc_pattern = _converters[type(pattern)](pattern)
        if (enc_pattern, True) not in self._refs:
            ch = _Sender(self, enc_pattern,
                         is_pattern=True,
                         loop=self._loop)
            self._refs[(enc_pattern, True)] = ch
        return self._refs[(enc_pattern, True)]

    @property
    def channels(self):
        """Read-only channels dict."""
        return types.MappingProxyType({
            ch.name: ch for ch in self._refs.values()
            if not ch.is_pattern})

    @property
    def patterns(self):
        """Read-only patterns dict."""
        return types.MappingProxyType({
            ch.name: ch for ch in self._refs.values()
            if ch.is_pattern})

    async def get(self, *, encoding=None, decoder=None):
        """Wait for and return pub/sub message from one of channels.

        Return value is either:

        * tuple of two elements: channel & message;

        * tuple of three elements: pattern channel, (target channel & message);

        * or None in case Receiver is not active or has just been stopped.

        :raises aioredis.ChannelClosedError: If listener is stopped
            and all messages have been received.
        """
        assert decoder is None or callable(decoder), decoder
        if not self.is_active:
            if not self._running:   # inactive but running
                raise ChannelClosedError()
            return
        obj = await self._queue.get()
        if obj is EndOfStream:
            return
        ch, msg = obj
        if ch.is_pattern:
            dest_ch, msg = msg
        if encoding is not None:
            msg = msg.decode(encoding)
        if decoder is not None:
            msg = decoder(msg)
        if ch.is_pattern:
            return ch, (dest_ch, msg)
        return ch, msg

    async def wait_message(self):
        """Blocks until new message appear."""
        if not self._queue.empty():
            return True
        if not self._running:
            return False
        if self._waiter is None:
            self._waiter = self._loop.create_future()
        await self._waiter
        return self.is_active

    @property
    def is_active(self):
        """Returns True if listener has any active subscription."""
        if not self._queue.empty():
            return True
        # NOTE: this expression requires at least one subscriber
        #   to return True;
        return (self._running and
                any(ch.is_active for ch in self._refs.values()))

    def stop(self):
        """Stop receiving messages.

        All new messages after this call will be ignored,
        so you must call unsubscribe before stopping this listener.
        """
        self._running = False
        self._put_nowait(EndOfStream, sender=None)

    def iter(self, *, encoding=None, decoder=None):
        """Returns async iterator.

        Usage example:

        >>> async for ch, msg in mpsc.iter():
        ...     print(ch, msg)
        """
        return _IterHelper(self,
                           is_active=lambda r: r.is_active or r._running,
                           encoding=encoding,
                           decoder=decoder)

    # internal methods

    def _put_nowait(self, data, *, sender):
        if not self._running and data is not EndOfStream:
            logger.warning("Pub/Sub listener message after stop:"
                           " sender: %r, data: %r",
                           sender, data)
            return
        if data is not EndOfStream:
            data = (sender, data)
        self._queue.put_nowait(data)
        if self._waiter is not None:
            fut, self._waiter = self._waiter, None
            _set_result(fut, None, self)

    def _close(self, sender):
        self._refs.pop((sender.name, sender.is_pattern))


class _Sender(AbcChannel):
    """Write-Only Channel.

    Does not allow direct ``.get()`` calls.
    """

    def __init__(self, receiver, name, is_pattern, *, loop):
        self._receiver = receiver
        self._name = _converters[type(name)](name)
        self._is_pattern = is_pattern
        self._loop = loop
        self._closed = False

    def __repr__(self):
        return "<{} name:{!r}, is_pattern:{}, receiver:{!r}>".format(
            self.__class__.__name__,
            self._name, self._is_pattern, self._receiver)

    @property
    def name(self):
        """Encoded channel name or pattern."""
        return self._name

    @property
    def is_pattern(self):
        """Set to True if channel is subscribed to pattern."""
        return self._is_pattern

    @property
    def is_active(self):
        return not self._closed

    async def get(self, *, encoding=None, decoder=None):
        raise RuntimeError("MPSC channel does not allow direct get() calls")

    def put_nowait(self, data):
        self._receiver._put_nowait(data, sender=self)

    def close(self):
        # TODO: close() is exclusive so we can not share same _Sender
        # between different connections.
        # This needs to be fixed.
        if self._closed:
            return
        self._closed = True
        self._receiver._close(self)
