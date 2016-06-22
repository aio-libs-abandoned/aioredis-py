from .connection import RedisConnection, create_connection
from .commands import (
    Redis, create_redis,
    create_reconnecting_redis,
    create_redis_pool,
    GeoPoint, GeoMember,
    )
from .pool import ConnectionsPool, create_pool
from .pubsub import Channel
from .errors import (
    ConnectionClosedError,
    MultiExecError,
    PipelineError,
    ProtocolError,
    RedisError,
    ReplyError,
    ChannelClosedError,
    WatchVariableError,
    PoolClosedError,
    )


__version__ = '0.3.0'

RedisPool = ConnectionsPool

# make pyflakes happy
(create_connection, RedisConnection,
 create_redis, create_reconnecting_redis, Redis,
 create_redis_pool, create_pool,
 RedisPool, ConnectionsPool, Channel,
 RedisError, ProtocolError, ReplyError,
 PipelineError, MultiExecError, ConnectionClosedError,
 ChannelClosedError, WatchVariableError,
 PoolClosedError,
 GeoPoint, GeoMember,
 )
