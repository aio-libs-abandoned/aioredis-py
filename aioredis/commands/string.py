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
    def bitcount(self, key, start, end, *extra):
        """Count set bits in a string.
        """
        pass

    @asyncio.coroutine
    def bitop(self, operation, dest, key, *keys):
        """Perform bitwise operations between strings.
        """
        pass

    @asyncio.coroutine
    def bitpos(self):
        """Find first bit set or clear in a string.
        """
        pass

    @asyncio.coroutine
    def decr(self, key):
        """Decrement the integer value of a key by one.
        """
        pass

    @asyncio.coroutine
    def decrby(self, key, decrement):
        """Decrement the integer value of a key by the given number.
        """
        pass

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
        pass

    @asyncio.coroutine
    def getrange(self, key, start, end):
        """Get a substring of the string stored at a key.
        """
        pass

    @asyncio.coroutine
    def getset(self, key, value):
        """Set the string value of a key and return its old value.
        """
        pass

    @asyncio.coroutine
    def incr(self, key):
        """Increment the integer value of a key by one.
        """
        pass

    @asyncio.coroutine
    def incrby(self, key, value):
        """Increment the integer value of a key by the given amount.
        """
        pass

    @asyncio.coroutine
    def incrbyfloat(self, key, value):
        """Increment the float value of a key by the given amount.
        """
        pass

    @asyncio.coroutine
    def mget(self, key, *keys):
        """Get the values of all the given keys.
        """
        pass

    @asyncio.coroutine
    def mset(self, key, value, *pairs):
        """Set multiple keys to multiple values.
        """
        pass

    @asyncio.coroutine
    def msetnx(self, keys, value, *pairs):
        """Set multiple keys to multiple values, only if none of the keys exist
        """
        pass

    @asyncio.coroutine
    def psetex(self, key, milliseconds, value):
        """Set the value and expiration in milliseconds of a key.
        """
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
        pass

    @asyncio.coroutine
    def setex(self, key, seconds, value):
        """Set the value and expiration of a key.
        """
        pass

    @asyncio.coroutine
    def setnx(self, key, value):
        """Set the value of a key, only if the key does not exist.
        """
        pass

    @asyncio.coroutine
    def setrange(self, key, offset, value):
        """Overwrite part of a string at key starting at the specified offset.
        """
        pass

    @asyncio.coroutine
    def strlen(self, key):
        """Get the length of the value stored in a key.
        """
        pass
