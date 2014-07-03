import asyncio


class StringCommandsMixin:
    """String commands mixin.

    For commands details see: http://redis.io/commands/#string
    """

    @asyncio.coroutine
    def append(self, key, value):
        """Append a value to key.
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'APPEND', key, value))

    @asyncio.coroutine
    def bitcount(self, key, start=None, end=None):
        """Count set bits in a string.
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if start is None and end is not None:
            raise TypeError("both start and stop must be specified")
        elif start is not None and end is None:
            raise TypeError("both start and stop must be specified")
        elif start is not None and end is not None:
            args = (start, end)
        else:
            args = ()
        res = yield from self._conn.execute(b'BITCOUNT', key, *args)
        return res

    @asyncio.coroutine
    def bitop_and(self, dest, key, *keys):
        """Perform bitwise AND operations between strings.
        """
        if dest is None:
            raise TypeError("dest argument must not be None")
        if key is None:
            raise TypeError("key argument must not be None")
        result = yield from self._conn.execute(b'BITOP', b'AND',
                                               dest, key, *keys)
        return result

    @asyncio.coroutine
    def bitop_or(self, dest, key, *keys):
        """Perform bitwise OR operations between strings.
        """
        if dest is None:
            raise TypeError("dest argument must not be None")
        if key is None:
            raise TypeError("key argument must not be None")
        result = yield from self._conn.execute(b'BITOP', b'OR',
                                               dest, key, *keys)
        return result

    @asyncio.coroutine
    def bitop_xor(self, dest, key, *keys):
        """Perform bitwise XOR operations between strings.
        """
        if dest is None:
            raise TypeError("dest argument must not be None")
        if key is None:
            raise TypeError("key argument must not be None")
        result = yield from self._conn.execute(b'BITOP', b'XOR',
                                               dest, key, *keys)
        return result

    @asyncio.coroutine
    def bitop_not(self, dest, key):
        """Perform bitwise NOT operations between strings.
        """
        if dest is None:
            raise TypeError("dest argument must not be None")
        if key is None:
            raise TypeError("key argument must not be None")
        result = yield from self._conn.execute(b'BITOP', b'NOT', dest, key)
        return result

    @asyncio.coroutine
    def bitpos(self):
        """Find first bit set or clear in a string.
        """
        raise NotImplementedError

    @asyncio.coroutine
    def decr(self, key):
        """Decrement the integer value of a key by one.
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'DECR', key))

    @asyncio.coroutine
    def decrby(self, key, decrement):
        """Decrement the integer value of a key by the given number.
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(decrement, int):
            raise TypeError("decrement must be of type int")
        return (yield from self._conn.execute(b'DECRBY', key, decrement))

    @asyncio.coroutine
    def get(self, key):
        """Get the value of a key.
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'GET', key))

    @asyncio.coroutine
    def getbit(self, key, offset):
        """Returns the bit value at offset in the string value stored at key.
        """
        raise NotImplementedError

    @asyncio.coroutine
    def getrange(self, key, start, end):
        """Get a substring of the string stored at a key.
        """
        raise NotImplementedError

    @asyncio.coroutine
    def getset(self, key, value):
        """Set the string value of a key and return its old value.
        """
        raise NotImplementedError
        pass

    @asyncio.coroutine
    def incr(self, key):
        """Increment the integer value of a key by one.
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'INCR', key))

    @asyncio.coroutine
    def incrby(self, key, increment):
        """Increment the integer value of a key by the given amount.
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(increment, int):
            raise TypeError("increment must be of type int")
        return (yield from self._conn.execute(b'INCRBY', key, increment))

    @asyncio.coroutine
    def incrbyfloat(self, key, increment):
        """Increment the float value of a key by the given amount.
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(increment, float):
            raise TypeError("increment must be of type int")
        result = yield from self._conn.execute(
            b'INCRBYFLOAT', key, increment)
        return float(result)

    @asyncio.coroutine
    def mget(self, key, *keys):
        """Get the values of all the given keys.
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if any(k is None for k in keys):
            raise TypeError("keys must not be None")
        return (yield from self._conn.execute(b'MGET', key, *keys))

    @asyncio.coroutine
    def mset(self, key, value, *pairs):
        """Set multiple keys to multiple values.
        """
        raise NotImplementedError
        pass

    @asyncio.coroutine
    def msetnx(self, keys, value, *pairs):
        """Set multiple keys to multiple values, only if none of the keys exist
        """
        raise NotImplementedError
        pass

    @asyncio.coroutine
    def psetex(self, key, milliseconds, value):
        """Set the value and expiration in milliseconds of a key.
        """
        raise NotImplementedError
        pass

    @asyncio.coroutine
    def set(self, key, value):
        """Set the string value of a key.
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'SET', key, value))

    @asyncio.coroutine
    def setbit(self, key, offset, value):
        """Sets or clears the bit at offset in the string value stored at key.
        """
        raise NotImplementedError
        pass

    @asyncio.coroutine
    def setex(self, key, seconds, value):
        """Set the value and expiration of a key.
        """
        raise NotImplementedError
        pass

    @asyncio.coroutine
    def setnx(self, key, value):
        """Set the value of a key, only if the key does not exist.
        """
        raise NotImplementedError
        pass

    @asyncio.coroutine
    def setrange(self, key, offset, value):
        """Overwrite part of a string at key starting at the specified offset.
        """
        raise NotImplementedError
        pass

    @asyncio.coroutine
    def strlen(self, key):
        """Get the length of the value stored in a key.
        """
        raise NotImplementedError
        pass
