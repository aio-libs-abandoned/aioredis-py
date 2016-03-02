__all__ = [
    'RedisError',
    'ProtocolError',
    'ReplyError',
    'PipelineError',
    'MultiExecError',
    'ConnectionClosedError',
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


class ChannelClosedError(RedisError):
    """Raised when Pub/Sub channel is unsubscribed and messages queue is empty.
    """


class ConnectionClosedError(RedisError):
    """Raised if connection to server was closed.
    """
