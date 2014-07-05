import asyncio


class ListCommandsMixin:
    """List commands mixin.

    For commands details see: http://redis.io/commands#list
    """

    @asyncio.coroutine
    def blpop(self, key, *keys, timeout=0):
        """Remove and get the first element in a list, or block until
        one is available"""
        if key is None:
            raise TypeError("key argument must not be None")
        args = keys + (timeout,)
        return (yield from self._conn.execute(b'BLPOP', key, *args))

    @asyncio.coroutine
    def brpop(self, key, *keys, timeout=0):
        """Remove and get the last element in a list, or block until one
        is available"""
        if key is None:
            raise TypeError("key argument must not be None")
        args = keys + (timeout,)
        return (yield from self._conn.execute(b'BRPOP', key, *args))

    @asyncio.coroutine
    def brpoplpush(self, sourcekey, destkey, timeout=0):
        """Remove and get the last element in a list, or block until one
        is available"""
        if sourcekey is None:
            raise TypeError("sourcekey argument must not be None")
        if destkey is None:
            raise TypeError("destkey argument must not be None")

        return (yield from self._conn.execute(
            b'BRPOPLPUSH', sourcekey, destkey, timeout))

    @asyncio.coroutine
    def lindex(self, key, index):
        """Get an element from a list by its index"""
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'LINDEX', key, index))

    @asyncio.coroutine
    def linsert(self, key, pivot, value, before=False):
        """Inserts value in the list stored at key either before or
        after the reference value pivot."""
        if key is None:
            raise TypeError("key argument must not be None")
        where = b'AFTER' if not before else b'BEFORE'
        return (yield from self._conn.execute(
            b'LINSERT', key, where, pivot, value))

    @asyncio.coroutine
    def llen(self, key):
        """Returns the length of the list stored at key."""
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'LLEN', key))

    @asyncio.coroutine
    def lpop(self, key):
        """Removes and returns the first element of the list stored at key."""
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'LPOP', key))

    @asyncio.coroutine
    def lpush(self, key, value, *values):
        """Insert all the specified values at the head of the list
        stored at key."""
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'LPUSH', key, value, *values))

    @asyncio.coroutine
    def lpushx(self, key, value):
        """Inserts value at the head of the list stored at key, only if key
        already exists and holds a list"""
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'LPUSHX', key, value))

    @asyncio.coroutine
    def lrange(self, key, start, stop):
        """Returns the specified elements of the list stored at key."""
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'LRANGE', key, start, stop))




    @asyncio.coroutine
    def rpush(self, key, value, *values):
        return (yield from self._conn.execute(b'RPUSH', key, value, *values))

    @asyncio.coroutine
    def lset(self, key, index, value,):
        return (yield from self._conn.execute(b'LSET', key, index, value))

