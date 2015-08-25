from .connection import RedisConnection, create_connection
from .commands import Redis, create_redis, create_reconnecting_redis
from .pool import RedisPool, create_pool
from .util import Channel
from .sentinel import RedisSentinel, create_sentinel
from .errors import (
    RedisError,
    ProtocolError,
    ReplyError,
    PipelineError,
    MultiExecError,
    ReadOnlyError,
    MasterNotFoundError,
    SlaveNotFoundError
    )


__version__ = '0.2.3'

# make pyflakes happy
(create_connection, RedisConnection,
 create_redis, create_reconnecting_redis, Redis,
 create_pool, RedisPool, Channel,
 create_sentinel, RedisSentinel,
 RedisError, ProtocolError, ReplyError,
 PipelineError, MultiExecError, MasterNotFoundError,
 SlaveNotFoundError, ReadOnlyError)
