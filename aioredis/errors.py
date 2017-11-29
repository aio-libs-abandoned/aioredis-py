__all__ = [
    'RedisError',
    'ProtocolError',
    'ReplyError',
    'MaxClientsError',
    'AuthError',
    'PipelineError',
    'MultiExecError',
    'WatchVariableError',
    'ChannelClosedError',
    'ConnectionClosedError',
    'ConnectionForcedCloseError',
    'PoolClosedError',
    'MasterNotFoundError',
    'SlaveNotFoundError',
    'ReadOnlyError',
    ]


class RedisError(Exception):
    """Base exception class for aioredis exceptions."""


class ProtocolError(RedisError):
    """Raised when protocol error occurs."""


class ReplyError(RedisError):
    """Raised for redis error replies (-ERR)."""

    MATCH_REPLY = None

    def __new__(cls, msg, *args):
        for klass in cls.__subclasses__():
            if msg and klass.MATCH_REPLY and msg.startswith(klass.MATCH_REPLY):
                return klass(msg, *args)
        return super().__new__(cls, msg, *args)


class MaxClientsError(ReplyError):
    """Raised for redis server when the maximum number of client has been
    reached."""

    MATCH_REPLY = "ERR max number of clients reached"


class AuthError(ReplyError):
    """Raised when authentication errors occurs."""

    MATCH_REPLY = ("NOAUTH ", "ERR invalid password")


class PipelineError(RedisError):
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


class ReadOnlyError(RedisError):
    """Raised from slave when read-only mode is enabled"""


class MasterNotFoundError(RedisError):
    """Raised for sentinel master not found error."""


class SlaveNotFoundError(RedisError):
    """Raised for sentinel slave not found error."""


class MasterReplyError(RedisError):
    """Raised by sentinel client for master error replies."""


class SlaveReplyError(RedisError):
    """Raised by sentinel client for slave error replies."""


class ConnectionClosedError(RedisError):
    """Raised if connection to server was closed."""


class ConnectionForcedCloseError(ConnectionClosedError):
    """Raised if connection was closed with .close() method."""


class PoolClosedError(RedisError):
    """Raised if pool is closed."""
