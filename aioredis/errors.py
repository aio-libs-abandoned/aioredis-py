__all__ = [
    'RedisError',
    'ProtocolError',
    'ReplyError',
    'MultiExecError',
    ]


class RedisError(Exception):
    """Base exception class for aioredis exceptions."""


class ProtocolError(RedisError):
    """Raised when protocol error occurs."""


class ReplyError(RedisError):
    """Raised for redis error replies (-ERR)."""


class MultiExecError(ReplyError):
    """Raised if command within MULTI/EXEC block caused error."""

    def __init__(self, errors):
        super().__init__('Got following error(s): ' +
                         '\n'.join('{!r}'.format(err) for err in errors))


# TODO: add ConnectionClosed exception.
