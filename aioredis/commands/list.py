import asyncio


class ListCommandsMixin:
    """List commands mixin.

    For commands details see: http://redis.io/commands#list
    """

    @asyncio.coroutine
    def blpop(self, key, *keys, timeout=0):
        """Remove and get the first element in a list, or block until
        one is available.

        :raises TypeError: if key is None or timeout is not int
        :raises ValueError: if timeout is less then 0
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(timeout, int):
            raise TypeError("timeout argument must be int")
        if timeout < 0:
            raise ValueError("timeout must be greater equal 0")
        args = keys + (timeout,)
        return (yield from self._conn.execute(b'BLPOP', key, *args))

    @asyncio.coroutine
    def brpop(self, key, *keys, timeout=0):
        """Remove and get the last element in a list, or block until one
        is available.

        :raises TypeError: if key is None or timeout is not int
        :raises ValueError: if timeout is less then 0
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(timeout, int):
            raise TypeError("timeout argument must be int")
        if timeout < 0:
            raise ValueError("timeout must be greater equal 0")
        args = keys + (timeout,)
        return (yield from self._conn.execute(b'BRPOP', key, *args))

    @asyncio.coroutine
    def brpoplpush(self, sourcekey, destkey, timeout=0):
        """Remove and get the last element in a list, or block until one
        is available.

        :raises TypeError: if sourcekey or destkey is None
        :raises TypeError: if timeout is not int
        :raises ValueError: if timeout is less then 0
        """
        if sourcekey is None:
            raise TypeError("sourcekey argument must not be None")
        if destkey is None:
            raise TypeError("destkey argument must not be None")

        if not isinstance(timeout, int):
            raise TypeError("timeout argument must be int")
        if timeout < 0:
            raise ValueError("timeout must be greater equal 0")

        return (yield from self._conn.execute(
            b'BRPOPLPUSH', sourcekey, destkey, timeout))

    @asyncio.coroutine
    def lindex(self, key, index):
        """Get an element from a list by its index.

        :raises TypeError: if key is None or index is not int
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(index, int):
            raise TypeError("index argument must be int")
        return (yield from self._conn.execute(b'LINDEX', key, index))

    @asyncio.coroutine
    def linsert(self, key, pivot, value, before=False):
        """Inserts value in the list stored at key either before or
        after the reference value pivot.

        :raises TypeError: if key is None
        """
        if key is None:
            raise TypeError("key argument must not be None")
        where = b'AFTER' if not before else b'BEFORE'
        return (yield from self._conn.execute(
            b'LINSERT', key, where, pivot, value))

    @asyncio.coroutine
    def llen(self, key):
        """Returns the length of the list stored at key.

        :raises TypeError: if key is None
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'LLEN', key))

    @asyncio.coroutine
    def lpop(self, key):
        """Removes and returns the first element of the list stored at key.

        :raises TypeError: if key is None
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'LPOP', key))

    @asyncio.coroutine
    def lpush(self, key, value, *values):
        """Insert all the specified values at the head of the list
        stored at key.

        :raises TypeError: if key is None
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'LPUSH', key, value, *values))

    @asyncio.coroutine
    def lpushx(self, key, value):
        """Inserts value at the head of the list stored at key, only if key
        already exists and holds a list.

        :raises TypeError: if key is None
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'LPUSHX', key, value))

    @asyncio.coroutine
    def lrange(self, key, start, stop):
        """Returns the specified elements of the list stored at key.

        :raises TypeError: if key is None
        :raises TypeError: if start or stop is not int
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(start, int):
            raise TypeError("start argument must be int")
        if not isinstance(stop, int):
            raise TypeError("stop argument must be int")
        return (yield from self._conn.execute(b'LRANGE', key, start, stop))

    @asyncio.coroutine
    def lrem(self, key, count, value):
        """Removes the first count occurrences of elements equal to value
        from the list stored at key.

        :raises TypeError: if key is None or count is not int
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(count, int):
            raise TypeError("count argument must be int")
        return (yield from self._conn.execute(b'LREM', key, count, value))

    @asyncio.coroutine
    def lset(self, key, index, value):
        """Sets the list element at index to value.

        :raises TypeError: if key is None or index is not int
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(index, int):
            raise TypeError("index argument must be int")
        return (yield from self._conn.execute(b'LSET', key, index, value))

    @asyncio.coroutine
    def ltrim(self, key, start, stop):
        """Trim an existing list so that it will contain only the specified
        range of elements specified.

        :raises TypeError: if key is None
        :raises TypeError: if start or stop is not int
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(start, int):
            raise TypeError("start argument must be int")
        if not isinstance(stop, int):
            raise TypeError("stop argument must be int")
        return (yield from self._conn.execute(b'LTRIM', key, start, stop))

    @asyncio.coroutine
    def rpop(self, key):
        """Removes and returns the last element of the list stored at key.

        :raises TypeError: if key is None
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'RPOP', key))

    @asyncio.coroutine
    def rpoplpush(self, sourcekey, destkey):
        """Atomically returns and removes the last element (tail) of the
        list stored at source, and pushes the element at the first element
        (head) of the list stored at destination.

        :raises TypeError: if sourcekey or destkey is None
        """
        if sourcekey is None:
            raise TypeError("sourcekey argument must not be None")
        if destkey is None:
            raise TypeError("destkey argument must not be None")
        return (yield from self._conn.execute(
            b'RPOPLPUSH', sourcekey, destkey))

    @asyncio.coroutine
    def rpush(self, key, value, *values):
        """Insert all the specified values at the tail of the list
        stored at key.

        :raises TypeError: if key is None
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'RPUSH', key, value, *values))

    @asyncio.coroutine
    def rpushx(self, key, value):
        """Inserts value at the tail of the list stored at key, only if
        key already exists and holds a list.

        :raises TypeError: if key is None
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'RPUSHX', key, value))
