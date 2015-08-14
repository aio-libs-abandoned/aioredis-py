from .connection import RedisConnection, create_connection
from .commands import Redis, create_redis, create_reconnecting_redis
from .pool import RedisPool, create_pool
from .util import Channel
from .errors import (
    RedisError,
    ProtocolError,
    ReplyError,
    PipelineError,
    MultiExecError,
    )


__version__ = '0.2.3'

# make pyflakes happy
(create_connection, RedisConnection,
 create_redis, create_reconnecting_redis, Redis,
 create_pool, RedisPool, Channel,
 RedisError, ProtocolError, ReplyError,
 PipelineError, MultiExecError)
