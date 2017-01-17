from .connection import RedisConnection, create_connection
from .commands import (
    Redis, create_redis,
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

__all__ = [
    # Factories
    'create_connection',
    'create_pool',
    'create_redis',
    'create_redis_pool',
    'create_sentinel',
    # Classes
    'RedisConnection',
    'ConnectionsPool',
    'Redis',
    'GeoPoint',
    'GeoMember',
    'Channel',
    'RedisSentinel',
    # Errors
    'RedisError',
    'ReplyError',
    'ProtocolError',
    'PipelineError',
    'MultiExecError',
    'WatchVariableError',
    'ConnectionClosedError',
    'PoolClosedError',
    'ChannelClosedError',
    'MasterNotFoundError',
    'SlaveNotFoundError',
    'ReadOnlyError',
]

# NOTE: this is deprecated
create_reconnecting_redis = create_pool

RedisPool = ConnectionsPool
