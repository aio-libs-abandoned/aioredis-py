from .connection import RedisConnection, create_connection
from .commands import (
    Redis, create_redis,
    create_reconnecting_redis,
    create_redis_pool,
    GeoPoint, GeoMember,
    )
from .pool import ConnectionsPool, create_pool
from .pubsub import Channel
from .sentinel import RedisSentinel, create_sentinel
from .errors import (
    ConnectionClosedError,
    MasterNotFoundError,
    MultiExecError,
    PipelineError,
    ProtocolError,
    ReadOnlyError,
    RedisError,
    ReplyError,
    ChannelClosedError,
    WatchVariableError,
    PoolClosedError,
    SlaveNotFoundError,
    )


__version__ = '1.0.0b1'

RedisPool = ConnectionsPool

# make pyflakes happy
(create_connection, RedisConnection,
 create_redis, create_reconnecting_redis, Redis,
 create_redis_pool, create_pool,
 create_sentinel, RedisSentinel,
 RedisPool, ConnectionsPool, Channel,
 RedisError, ProtocolError, ReplyError,
 PipelineError, MultiExecError, ConnectionClosedError,
 ChannelClosedError, WatchVariableError, PoolClosedError,
 MasterNotFoundError, SlaveNotFoundError, ReadOnlyError,
 GeoPoint, GeoMember,
 )
