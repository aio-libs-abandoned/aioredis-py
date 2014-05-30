from .connection import RedisConnection, create_connection
from .commands import Redis, create_redis
from .errors import RedisError, ProtocolError, ReplyError


__version__ = '0.0.1'

# make pyflakes happy
(create_connection, RedisConnection,
 create_redis, Redis,
 RedisError, ProtocolError, ReplyError)
