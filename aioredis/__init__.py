from .connection import RedisConnection, create_connection
from .errors import RedisError, ProtocolError, ReplyError


__version__ = '0.0.1'

# make pyflakes happy
(create_connection, RedisConnection,
 RedisError, ProtocolError, ReplyError)
