import asyncio
import json
import types
import collections

from .abc import AbcChannel
from .util import _converters   # , _set_result
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

    def __init__(self, name, is_pattern, loop=None):
        self._queue = ClosableQueue(loop=loop)
        self._name = _converters[type(name)](name)
        self._is_pattern = is_pattern

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
        return not self._queue.exhausted

    async def get(self, *, encoding=None, decoder=None):
        """Coroutine that waits for and returns a message.

        :raises aioredis.ChannelClosedError: If channel is unsubscribed
            and has no messages.
        """
        assert decoder is None or callable(decoder), decoder
        if self._queue.exhausted:
            raise ChannelClosedError()
        msg = await self._queue.get()
        if msg is EndOfStream:
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
        """Waits for message to become available in channel
        or channel is closed (unsubscribed).

        Possible usage:

        >>> while (await ch.wait_message()):
        ...     msg = await ch.get()
        """
        if not self.is_active:
            return False
        if not self._queue.empty():
            return True
        await self._queue.wait()
        return self.is_active

    # internal methods

    def put_nowait(self, data):
        self._queue.put(data)

    def close(self, exc=None):
        """Marks channel as inactive.

        Internal method, will be called from connection
        on `unsubscribe` command.
        """
        if not self._queue.closed:
            self._queue.close()


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

    def __init__(self, loop=None, on_close=None):
        assert on_close is None or callable(on_close), (
            "on_close must be None or callable", on_close)
        if loop is None:
            loop = asyncio.get_event_loop()
        if on_close is None:
            on_close = self.check_stop
        self._queue = ClosableQueue(loop=loop)
        self._refs = {}
        self._on_close = on_close

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
                         is_pattern=False)
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
                         is_pattern=True)
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
        # TODO: add note about raised exception and end marker.
        #   Flow before ClosableQueue:
        #   - ch.get() -> message
        #   - ch.close() -> ch.put(None)
        #   - ch.get() -> None
        #   - ch.get() -> ChannelClosedError
        #   Current flow:
        #   - ch.get() -> message
        #   - ch.close() -> ch._closed = True
        #   - ch.get() -> ChannelClosedError
        assert decoder is None or callable(decoder), decoder
        if self._queue.exhausted:
            raise ChannelClosedError()
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
        if self._queue.closed:
            return False
        await self._queue.wait()
        return self.is_active

    @property
    def is_active(self):
        """Returns True if listener has any active subscription."""
        if self._queue.exhausted:
            return False
        return any(ch.is_active for ch in self._refs.values())

    def stop(self):
        """Stop receiving messages.

        All new messages after this call will be ignored,
        so you must call unsubscribe before stopping this listener.
        """
        self._queue.close()
        # TODO: discard all senders as they might still be active.
        #   Channels storage in Connection should be refactored:
        #   if we drop _Senders here they will still be subscribed
        #   and will reside in memory although messages will be discarded.

    def iter(self, *, encoding=None, decoder=None):
        """Returns async iterator.

        Usage example:

        >>> async for ch, msg in mpsc.iter():
        ...     print(ch, msg)
        """
        return _IterHelper(self,
                           is_active=lambda r: not r._queue.exhausted,
                           encoding=encoding,
                           decoder=decoder)

    def check_stop(self, channel, exc=None):
        """TBD"""
        # NOTE: this is a fast-path implementation,
        # if overridden, implementation should use public API:
        #
        # if self.is_active and not (self.channels or self.patterns):
        if not self._refs:
            self.stop()

    # internal methods

    def _put_nowait(self, data, *, sender):
        if self._queue.closed:
            logger.warning("Pub/Sub listener message after stop:"
                           " sender: %r, data: %r",
                           sender, data)
            return
        self._queue.put((sender, data))

    def _close(self, sender, exc=None):
        self._refs.pop((sender.name, sender.is_pattern))
        self._on_close(sender, exc=exc)


class _Sender(AbcChannel):
    """Write-Only Channel.

    Does not allow direct ``.get()`` calls.
    """

    def __init__(self, receiver, name, is_pattern):
        self._receiver = receiver
        self._name = _converters[type(name)](name)
        self._is_pattern = is_pattern
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

    def close(self, exc=None):
        # TODO: close() is exclusive so we can not share same _Sender
        # between different connections.
        # This needs to be fixed.
        if self._closed:
            return
        self._closed = True
        self._receiver._close(self, exc=exc)


class ClosableQueue:

    def __init__(self, *, loop=None):
        self._queue = collections.deque()
        self._event = asyncio.Event(loop=loop)
        self._closed = False

    async def wait(self):
        while not (self._queue or self._closed):
            await self._event.wait()
        return True

    async def get(self):
        await self.wait()
        assert self._queue or self._closed, (
            "Unexpected queue state", self._queue, self._closed)
        if not self._queue and self._closed:
            return EndOfStream
        item = self._queue.popleft()
        if not self._queue:
            self._event.clear()
        return item

    def put(self, item):
        if self._closed:
            return
        self._queue.append(item)
        self._event.set()

    def close(self):
        """Mark queue as closed and notify all waiters."""
        self._closed = True
        self._event.set()

    @property
    def closed(self):
        return self._closed

    @property
    def exhausted(self):
        return self._closed and not self._queue

    def empty(self):
        return not self._queue

    def qsize(self):
        return len(self._queue)

    def __repr__(self):
        closed = 'closed' if self._closed else 'open'
        return '<Queue {} size:{}>'.format(closed, len(self._queue))
