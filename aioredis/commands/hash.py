import asyncio


class HashCommandsMixin:
    """Hash commands mixin.

    For commands details see: http://redis.io/commands#hash
    """

    @asyncio.coroutine
    def hdel(self, key, field, *fields):
        """Delete one or more hash fields."""
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'HDEL', key, field, *fields))

    @asyncio.coroutine
    def hexists(self, key, field):
        """Determine if hash field exists."""
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'HEXISTS', key, field))

    @asyncio.coroutine
    def hget(self, key, field):
        """Get the value of a hash field."""
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'HGET', key, field))

    @asyncio.coroutine
    def hgetall(self, key):
        """Get all the fields and values in a hash."""
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'HGETALL', key))

    @asyncio.coroutine
    def hincrby(self, key, field, increment=1):
        """Increment the integer value of a hash field by the given number."""
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(
            b'HINCRBY', key, field, increment))

    @asyncio.coroutine
    def hincrbyfloat(self, key, field, increment=1.0):
        """Increment the float value of a hash field by the given number."""
        if key is None:
            raise TypeError("key argument must not be None")
        result = yield from self._conn.execute(
            b'HINCRBYFLOAT', key, field, increment)
        return float(result)

    @asyncio.coroutine
    def hkeys(self, key):
        """Get all the fields in a hash."""
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'HKEYS', key))

    @asyncio.coroutine
    def hlen(self, key):
        """Get the number of fields in a hash."""
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'HLEN', key))

    @asyncio.coroutine
    def hmget(self, key, field, *fields):
        """Get the values of all the given fields."""
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'HMGET', key, field, *fields))

    @asyncio.coroutine
    def hmset(self, key, field, value, *pairs):
        """Set multiple hash fields to multiple values."""
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(
            b'HMSET', key, field, value, *pairs))

    @asyncio.coroutine
    def hset(self, key, field, value):
        """Set the string value of a hash field."""
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'HSET', key, field, value))

    @asyncio.coroutine
    def hsetnx(self, key, field, value):
        """Set the value of a hash field, only if the field does not exist."""
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'HSETNX', key, field, value))

    @asyncio.coroutine
    def hvals(self, key):
        """Get all the values in a hash."""
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'HVALS', key))

    @asyncio.coroutine
    def hscan(self, key, cursor=0, match=None, count=None):
        """Incrementally iterate hash fields and associated values."""
        if key is None:
            raise TypeError("key argument must not be None")
        args = [key, cursor]
        match is not None and args.extend([b'MATCH', match])
        count is not None and args.extend([b'COUNT', count])
        cursor, value = yield from self._conn.execute(b'HSCAN', *args)
        return int(cursor), value
