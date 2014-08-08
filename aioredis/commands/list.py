

class ListCommandsMixin:
    """List commands mixin.

    For commands details see: http://redis.io/commands#list
    """

    def blpop(self, key, *keys, timeout=0):
        """Remove and get the first element in a list, or block until
        one is available.

        :raises TypeError: if timeout is not int
        :raises ValueError: if timeout is less then 0
        """
        if not isinstance(timeout, int):
            raise TypeError("timeout argument must be int")
        if timeout < 0:
            raise ValueError("timeout must be greater equal 0")
        args = keys + (timeout,)
        return self._conn.execute(b'BLPOP', key, *args)

    def brpop(self, key, *keys, timeout=0):
        """Remove and get the last element in a list, or block until one
        is available.

        :raises TypeError: if timeout is not int
        :raises ValueError: if timeout is less then 0
        """
        if not isinstance(timeout, int):
            raise TypeError("timeout argument must be int")
        if timeout < 0:
            raise ValueError("timeout must be greater equal 0")
        args = keys + (timeout,)
        return self._conn.execute(b'BRPOP', key, *args)

    def brpoplpush(self, sourcekey, destkey, timeout=0):
        """Remove and get the last element in a list, or block until one
        is available.

        :raises TypeError: if timeout is not int
        :raises ValueError: if timeout is less then 0
        """
        if not isinstance(timeout, int):
            raise TypeError("timeout argument must be int")
        if timeout < 0:
            raise ValueError("timeout must be greater equal 0")
        return self._conn.execute(b'BRPOPLPUSH', sourcekey, destkey, timeout)

    def lindex(self, key, index):
        """Get an element from a list by its index.

        :raises TypeError: if index is not int
        """
        if not isinstance(index, int):
            raise TypeError("index argument must be int")
        return self._conn.execute(b'LINDEX', key, index)

    def linsert(self, key, pivot, value, before=False):
        """Inserts value in the list stored at key either before or
        after the reference value pivot.
        """
        where = b'AFTER' if not before else b'BEFORE'
        return self._conn.execute(b'LINSERT', key, where, pivot, value)

    def llen(self, key):
        """Returns the length of the list stored at key."""
        return self._conn.execute(b'LLEN', key)

    def lpop(self, key):
        """Removes and returns the first element of the list stored at key."""
        return self._conn.execute(b'LPOP', key)

    def lpush(self, key, value, *values):
        """Insert all the specified values at the head of the list
        stored at key.
        """
        return self._conn.execute(b'LPUSH', key, value, *values)

    def lpushx(self, key, value):
        """Inserts value at the head of the list stored at key, only if key
        already exists and holds a list.
        """
        return self._conn.execute(b'LPUSHX', key, value)

    def lrange(self, key, start, stop):
        """Returns the specified elements of the list stored at key.

        :raises TypeError: if start or stop is not int
        """
        if not isinstance(start, int):
            raise TypeError("start argument must be int")
        if not isinstance(stop, int):
            raise TypeError("stop argument must be int")
        return self._conn.execute(b'LRANGE', key, start, stop)

    def lrem(self, key, count, value):
        """Removes the first count occurrences of elements equal to value
        from the list stored at key.

        :raises TypeError: if count is not int
        """
        if not isinstance(count, int):
            raise TypeError("count argument must be int")
        return self._conn.execute(b'LREM', key, count, value)

    def lset(self, key, index, value):
        """Sets the list element at index to value.

        :raises TypeError: if index is not int
        """
        if not isinstance(index, int):
            raise TypeError("index argument must be int")
        return self._conn.execute(b'LSET', key, index, value)

    def ltrim(self, key, start, stop):
        """Trim an existing list so that it will contain only the specified
        range of elements specified.

        :raises TypeError: if start or stop is not int
        """
        if not isinstance(start, int):
            raise TypeError("start argument must be int")
        if not isinstance(stop, int):
            raise TypeError("stop argument must be int")
        return self._conn.execute(b'LTRIM', key, start, stop)

    def rpop(self, key):
        """Removes and returns the last element of the list stored at key."""
        return self._conn.execute(b'RPOP', key)

    def rpoplpush(self, sourcekey, destkey):
        """Atomically returns and removes the last element (tail) of the
        list stored at source, and pushes the element at the first element
        (head) of the list stored at destination.
        """
        return self._conn.execute(b'RPOPLPUSH', sourcekey, destkey)

    def rpush(self, key, value, *values):
        """Insert all the specified values at the tail of the list
        stored at key.
        """
        return self._conn.execute(b'RPUSH', key, value, *values)

    def rpushx(self, key, value):
        """Inserts value at the tail of the list stored at key, only if
        key already exists and holds a list.
        """
        return self._conn.execute(b'RPUSHX', key, value)
