"""The module provides connection and connections pool interfaces.

These are intended to be used for implementing custom connection managers.
"""
import abc


__all__ = [
    'AbcConnection',
    'AbcPool',
    'AbcChannel',
]


class AbcConnection(abc.ABC):
    """Abstract connection interface."""

    @abc.abstractmethod
    def execute(self, command, *args, **kwargs):
        """Execute redis command."""

    @abc.abstractmethod
    def execute_pubsub(self, command, *args, **kwargs):
        """Execute Redis (p)subscribe/(p)unsubscribe commands."""

    @abc.abstractmethod
    def close(self):
        """Perform connection(s) close and resources cleanup."""

    @abc.abstractmethod
    async def wait_closed(self):
        """
        Coroutine waiting until all resources are closed/released/cleaned up.
        """

    @property
    @abc.abstractmethod
    def closed(self):
        """Flag indicating if connection is closing or already closed."""

    @property
    @abc.abstractmethod
    def db(self):
        """Current selected DB index."""

    @property
    @abc.abstractmethod
    def encoding(self):
        """Current set connection codec."""

    @property
    @abc.abstractmethod
    def in_pubsub(self):
        """Returns number of subscribed channels.

        Can be tested as bool indicating Pub/Sub mode state.
        """

    @property
    @abc.abstractmethod
    def pubsub_channels(self):
        """Read-only channels dict."""

    @property
    @abc.abstractmethod
    def pubsub_patterns(self):
        """Read-only patterns dict."""

    @property
    @abc.abstractmethod
    def address(self):
        """Connection address."""


class AbcPool(AbcConnection):
    """Abstract connections pool interface.

    Inherited from AbcConnection so both have common interface
    for executing Redis commands.
    """

    @abc.abstractmethod
    def get_connection(self, command, args=()):
        """
        Gets free connection from pool in a sync way.

        If no connection available — returns None.
        """

    @abc.abstractmethod
    async def acquire(self, command=None, args=()):
        """Acquires connection from pool."""

    @abc.abstractmethod
    def release(self, conn):
        """Releases connection to pool.

        :param AbcConnection conn: Owned connection to be released.
        """

    @property
    @abc.abstractmethod
    def address(self):
        """Connection address or None."""


class AbcChannel(abc.ABC):
    """Abstract Pub/Sub Channel interface."""

    @property
    @abc.abstractmethod
    def name(self):
        """Encoded channel name or pattern."""

    @property
    @abc.abstractmethod
    def is_pattern(self):
        """Boolean flag indicating if channel is pattern channel."""

    @property
    @abc.abstractmethod
    def is_active(self):
        """Flag indicating that channel has unreceived messages
        and not marked as closed."""

    @abc.abstractmethod
    async def get(self):
        """Wait and return new message.

        Will raise ``ChannelClosedError`` if channel is not active.
        """

    # wait_message is not required; details of implementation
    # @abc.abstractmethod
    # def wait_message(self):
    #     pass

    @abc.abstractmethod
    def put_nowait(self, data):
        """Send data to channel.

        Called by RedisConnection when new message received.
        For pattern subscriptions data will be a tuple of
        channel name and message itself.
        """

    @abc.abstractmethod
    def close(self, exc=None):
        """Marks Channel as closed, no more messages will be sent to it.

        Called by RedisConnection when channel is unsubscribed
        or connection is closed.
        """
