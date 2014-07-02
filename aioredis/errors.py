__all__ = [
    'RedisError',
    'ProtocolError',
    'ReplyError',
    'WrongArgumentError',
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


class WrongArgumentError(RedisError):
    """Raised when wrong arguments supplid to redis command.
    """
# TODO: add ConnectionClosed exception.
