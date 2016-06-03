__all__ = [
    'RedisError',
    'ProtocolError',
    'ReplyError',
    'PipelineError',
    'MultiExecError',
    'WatchVariableError',
    'ChannelClosedError',
    'ConnectionClosedError',
    'PoolClosedError',
    ]


class RedisError(Exception):
    """Base exception class for aioredis exceptions."""


class ProtocolError(RedisError):
    """Raised when protocol error occurs."""


class ReplyError(RedisError):
    """Raised for redis error replies (-ERR)."""


class PipelineError(ReplyError):
    """Raised if command within pipeline raised error."""

    def __init__(self, errors):
        super().__init__('{} errors:'.format(self.__class__.__name__), errors)


class MultiExecError(PipelineError):
    """Raised if command within MULTI/EXEC block caused error."""


class WatchVariableError(MultiExecError):
    """Raised if watched variable changed (EXEC returns None)."""


class ChannelClosedError(RedisError):
    """Raised when Pub/Sub channel is unsubscribed and messages queue is empty.
    """


class ConnectionClosedError(RedisError):
    """Raised if connection to server was closed."""


class PoolClosedError(RedisError):
    """Raised if pool is closed."""
