__all__ = [
    'RedisError',
    'ProtocolError',
    'ReplyError',
    'PipelineError',
    'MultiExecError',
    'MasterNotFoundError',
    'SlaveNotFoundError',
    'ReadOnlyError'
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


class ReadOnlyError(RedisError):
    """Raised from slave when read-only mode is enabled"""


class MasterNotFoundError(RedisError):
    """Raised for sentinel master not found error."""


class SlaveNotFoundError(RedisError):
    """Raised for sentinel slave not found error."""


# TODO: add ConnectionClosed exception.
