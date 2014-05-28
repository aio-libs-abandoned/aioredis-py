__all__ = [
    'RedisError',
    'ProtocolError',
    'ReplyError',
    ]


class RedisError(Exception):
    """Base exception class for aioredis exceptions.
    """


class ProtocolError(RedisError):
    """Raised when protocol error occurs.
    """


class ReplyError(RedisError):
    """Raised for redis error replies (-ERR).
    """

# TODO: add ConnectionClosed exception.
