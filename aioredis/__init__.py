from .connection import RedisConnection, create_connection
from .commands import Redis, create_redis, create_reconnecting_redis
from .pool import RedisPool, create_pool
from .cluster import (
    RedisPoolCluster,
    create_pool_cluster,
    create_cluster,
    RedisCluster,
)
from .util import Channel
from .errors import (
    ConnectionClosedError,
    MultiExecError,
    PipelineError,
    ProtocolError,
    RedisError,
    ReplyError,
    RedisClusterError,
    )


__version__ = '0.2.6'

# make pyflakes happy
(create_connection, RedisConnection,
 create_redis, create_reconnecting_redis, Redis,
 create_pool, RedisPool, Channel,
 create_pool_cluster, RedisPoolCluster, Channel,
 create_cluster, RedisCluster,
 RedisError, ProtocolError, ReplyError,
 PipelineError, MultiExecError, ConnectionClosedError,
 RedisClusterError)
