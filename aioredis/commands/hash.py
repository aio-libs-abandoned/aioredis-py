import asyncio


class HashCommandsMixin:
    """Hash commands mixin.

    For commands details see: http://redis.io/commands#hash
    """

    @asyncio.coroutine
    def hdel(self, key, field, *fields):
        """Delete one or more hash fields"""
        return (yield from self._conn.execute(b'HDEL', key, field, *fields))

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
        return (yield from self._conn.execute(
            b'HINCRBYFLOAT', key, field, increment))

    @asyncio.coroutine
    def hkeys(self, key):
        """Get all the fields in a hash"""
        return (yield from self._conn.execute(b'HKEYS', key))

    @asyncio.coroutine
    def hlen(self, key):
        """Returns the number of fields contained in the hash
        stored at key."""
        return (yield from self._conn.execute(b'HLEN', key))

    @asyncio.coroutine
    def hmget(self, key, field, *fields):
        """Returns the values associated with the specified fields in
        the hash stored at key."""
        return (yield from self._conn.execute(b'HMGET', key, field, *fields))

    @asyncio.coroutine
    def hmset(self, key, field, value, *pairs):
        """Sets the specified fields to their respective values in
        the hash stored at key."""
        if len(pairs) % 2:
            raise ValueError("HMSET length of pairs must be even number")
        return (yield from self._conn.execute(
            b'HMSET', key, field, value, *pairs))

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
