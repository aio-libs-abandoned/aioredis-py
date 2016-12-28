"""The module provides connection and connections pool interfaces.

These are intended to be used for implementing custom connection managers.
"""
import abc
import asyncio
try:
    from abc import ABC
except ImportError:
    class ABC(metaclass=abc.ABCMeta):
        pass


__all__ = [
    'AbcConnection',
    'AbcPool',
]


class AbcConnection(ABC):
    """Abstract connection interface."""

    @abc.abstractmethod
    def execute(self):
        """Execute redis command."""

    @abc.abstractmethod
    def execute_pubsub(self):
        """Execute Redis (p)subscribe/(p)unsubscribe commands."""

    @abc.abstractmethod
    def close(self):
        """Perform connection(s) close and resources cleanup."""

    @asyncio.coroutine
    @abc.abstractmethod
    def wait_closed(self):
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
        """Currently selected DB index."""

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

    Inherited from AbcConnection so both common interface
    for executing Redis commands.
    """

    @abc.abstractmethod
    def get_connection(self):   # TODO: arguments
        """Gets free connection from pool in a sync way.
        If no connection available — returns None
        """

    @asyncio.coroutine
    @abc.abstractmethod
    def acquire(self):  # TODO: arguments
        """Acquires connection from pool."""

    @abc.abstractmethod
    def release(self):  # TODO: arguments
        """Releases connection to pool."""

    @property
    @abc.abstractmethod
    def address(self):
        """Connection address or None."""
