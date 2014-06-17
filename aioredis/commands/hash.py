import asyncio


class HashCommandsMixin:
    """Hash commands mixin.

    For commands details see: http://redis.io/commands#hash
    """

    @asyncio.coroutine
    def hdel(self, key, *fields):
        """Delete one or more hash fields"""
        return (yield from self._conn.execute(b'HDEL', key, *fields))

    @asyncio.coroutine
    def hexists(self, key, field):
        """Delete one or more hash fields"""
        return (yield from self._conn.execute(b'HEXISTS', key, field))

    @asyncio.coroutine
    def hget(self, key, field):
        """Get the value of a hash field"""
        return (yield from self._conn.execute(b'HGET', key, field))

    @asyncio.coroutine
    def hgetall(self, key):
        """Get all the fields and values in a hash"""
        return (yield from self._conn.execute(b'HGETALL', key))

    @asyncio.coroutine
    def hincrby(self, key, field, increment=1):
        """Increment the integer value of a hash field by the given number"""
        return (yield from self._conn.execute(
            b'HINCRBY', key, field, increment))

    @asyncio.coroutine
    def hincrbyfloat(self, key, field, increment=1.0):
        """Increment the integer value of a hash field by the given number"""
        result = yield from self._conn.execute(b'HINCRBYFLOAT', key, field, increment)
        return float(result)

    @asyncio.coroutine
    def hkeys(self, key):
        """Get all the fields in a hash"""
        return (yield from self._conn.execute(b'HKEYS', key))

    @asyncio.coroutine
    def hlen(self, key):
        """Get all the fields in a hash"""
        return (yield from self._conn.execute(b'HLEN', key))

    @asyncio.coroutine
    def hmget(self, key, *fields):
        """Returns the values associated with the specified fields in
        the hash stored at key."""
        return (yield from self._conn.execute(b'HMGET', key, *fields))

    @asyncio.coroutine
    def hmset(self, key, mapping):
        """Set field to value within hash ``key`` for each corresponding
        field and value from the ``mapping`` dict."""
        if not mapping:
            raise ValueError("'hmset' with 'mapping' of length 0")
        items = [pair for pair in mapping.items()]
        return (yield from self._conn.execute(b'HMSET', key, items))

    @asyncio.coroutine
    def hset(self, key, field, value):
        """Set the string value of a hash field"""
        return (yield from self._conn.execute(b'HSET', key, field, value))

    @asyncio.coroutine
    def hsetnx(self, key, field, value):
        """Set the value of a hash field, only if the field does not exist"""
        return (yield from self._conn.execute(b'HSETNX', key, field, value))

    @asyncio.coroutine
    def hvals(self, key):
        """Get all the values in a hash"""
        return (yield from self._conn.execute(b'HVALS', key))

    @asyncio.coroutine
    def hscan(self, key, cursor=0, match=None, count=None):
        """Incrementally iterate hash fields and associated values"""
        tokens = [key, cursor]
        match is not None and tokens.extend([b'MATCH', match])
        count is not None and tokens.extend([b'COUNT', count])
        return (yield from self._conn.execute(b'HSCAN', *tokens))
