from aioredis.util import wait_convert, wait_ok, _NOTSET


class StringCommandsMixin:
    """String commands mixin.

    For commands details see: http://redis.io/commands/#string
    """

    SET_IF_NOT_EXIST = 'SET_IF_NOT_EXIST'   # NX
    SET_IF_EXIST = 'SET_IF_EXIST'           # XX

    def append(self, key, value):
        """Append a value to key."""
        return self._conn.execute(b'APPEND', key, value)

    def bitcount(self, key, start=None, end=None):
        """Count set bits in a string.

        :raises TypeError: if only start or end specified.
        """
        if start is None and end is not None:
            raise TypeError("both start and stop must be specified")
        elif start is not None and end is None:
            raise TypeError("both start and stop must be specified")
        elif start is not None and end is not None:
            args = (start, end)
        else:
            args = ()
        return self._conn.execute(b'BITCOUNT', key, *args)

    def bitop_and(self, dest, key, *keys):
        """Perform bitwise AND operations between strings."""
        return self._conn.execute(b'BITOP', b'AND', dest, key, *keys)

    def bitop_or(self, dest, key, *keys):
        """Perform bitwise OR operations between strings."""
        return self._conn.execute(b'BITOP', b'OR', dest, key, *keys)

    def bitop_xor(self, dest, key, *keys):
        """Perform bitwise XOR operations between strings."""
        return self._conn.execute(b'BITOP', b'XOR', dest, key, *keys)

    def bitop_not(self, dest, key):
        """Perform bitwise NOT operations between strings."""
        return self._conn.execute(b'BITOP', b'NOT', dest, key)

    def bitpos(self, key, bit, start=None, end=None):
        """Find first bit set or clear in a string.

        :raises ValueError: if bit is not 0 or 1
        """
        if bit not in (1, 0):
            raise ValueError("bit argument must be either 1 or 0")
        bytes_range = []
        if start is not None:
            bytes_range.append(start)
        if end is not None:
            if start is None:
                bytes_range = [0, end]
            else:
                bytes_range.append(end)
        return self._conn.execute(b'BITPOS', key, bit, *bytes_range)

    def decr(self, key):
        """Decrement the integer value of a key by one."""
        return self._conn.execute(b'DECR', key)

    def decrby(self, key, decrement):
        """Decrement the integer value of a key by the given number.

        :raises TypeError: if decrement is not int
        """
        if not isinstance(decrement, int):
            raise TypeError("decrement must be of type int")
        return self._conn.execute(b'DECRBY', key, decrement)

    def get(self, key, *, encoding=_NOTSET):
        """Get the value of a key."""
        return self._conn.execute(b'GET', key, encoding=encoding)

    def getbit(self, key, offset):
        """Returns the bit value at offset in the string value stored at key.

        :raises TypeError: if offset is not int
        :raises ValueError: if offset is less then 0
        """
        if not isinstance(offset, int):
            raise TypeError("offset argument must be int")
        if offset < 0:
            raise ValueError("offset must be greater equal 0")
        return self._conn.execute(b'GETBIT', key, offset)

    def getrange(self, key, start, end, *, encoding=_NOTSET):
        """Get a substring of the string stored at a key.

        :raises TypeError: if start or end is not int
        """
        if not isinstance(start, int):
            raise TypeError("start argument must be int")
        if not isinstance(end, int):
            raise TypeError("end argument must be int")
        return self._conn.execute(b'GETRANGE', key, start, end,
                                  encoding=encoding)

    def getset(self, key, value, *, encoding=_NOTSET):
        """Set the string value of a key and return its old value."""
        return self._conn.execute(b'GETSET', key, value, encoding=encoding)

    def incr(self, key):
        """Increment the integer value of a key by one."""
        return self._conn.execute(b'INCR', key)

    def incrby(self, key, increment):
        """Increment the integer value of a key by the given amount.

        :raises TypeError: if increment is not int
        """
        if not isinstance(increment, int):
            raise TypeError("increment must be of type int")
        return self._conn.execute(b'INCRBY', key, increment)

    def incrbyfloat(self, key, increment):
        """Increment the float value of a key by the given amount.

        :raises TypeError: if increment is not int
        """
        if not isinstance(increment, float):
            raise TypeError("increment must be of type int")
        fut = self._conn.execute(b'INCRBYFLOAT', key, increment)
        return wait_convert(fut, float)

    def mget(self, key, *keys, encoding=_NOTSET):
        """Get the values of all the given keys."""
        return self._conn.execute(b'MGET', key, *keys, encoding=encoding)

    def mset(self, key, value, *pairs):
        """Set multiple keys to multiple values.

        :raises TypeError: if len of pairs is not event number
        """
        if len(pairs) % 2 != 0:
            raise TypeError("length of pairs must be even number")
        fut = self._conn.execute(b'MSET', key, value, *pairs)
        return wait_ok(fut)

    def msetnx(self, key, value, *pairs):
        """Set multiple keys to multiple values,
        only if none of the keys exist.

        :raises TypeError: if len of pairs is not event number
        """
        if len(pairs) % 2 != 0:
            raise TypeError("length of pairs must be even number")
        return self._conn.execute(b'MSETNX', key, value, *pairs)

    def psetex(self, key, milliseconds, value):
        """Set the value and expiration in milliseconds of a key.

        :raises TypeError: if milliseconds is not int
        """
        if not isinstance(milliseconds, int):
            raise TypeError("milliseconds argument must be int")
        fut = self._conn.execute(b'PSETEX', key, milliseconds, value)
        return wait_ok(fut)

    def set(self, key, value, *, expire=0, pexpire=0, exist=None):
        """Set the string value of a key.

        :raises TypeError: if expire or pexpire is not int
        """
        if expire and not isinstance(expire, int):
            raise TypeError("expire argument must be int")
        if pexpire and not isinstance(pexpire, int):
            raise TypeError("pexpire argument must be int")

        args = []
        if expire:
            args[:] = [b'EX', expire]
        if pexpire:
            args[:] = [b'PX', pexpire]

        if exist is self.SET_IF_EXIST:
            args.append(b'XX')
        elif exist is self.SET_IF_NOT_EXIST:
            args.append(b'NX')
        fut = self._conn.execute(b'SET', key, value, *args)
        return wait_ok(fut)

    def setbit(self, key, offset, value):
        """Sets or clears the bit at offset in the string value stored at key.

        :raises TypeError: if offset is not int
        :raises ValueError: if offset is less then 0 or value is not 0 or 1
        """
        if not isinstance(offset, int):
            raise TypeError("offset argument must be int")
        if offset < 0:
            raise ValueError("offset must be greater equal 0")
        if value not in (0, 1):
            raise ValueError("value argument must be either 1 or 0")
        return self._conn.execute(b'SETBIT', key, offset, value)

    def setex(self, key, seconds, value):
        """Set the value and expiration of a key.

        If seconds is float it will be multiplied by 1000
        coerced to int and passed to `psetex` method.

        :raises TypeError: if seconds is neither int nor float
        """
        if isinstance(seconds, float):
            return self.psetex(key, int(seconds * 1000), value)
        if not isinstance(seconds, int):
            raise TypeError("milliseconds argument must be int")
        fut = self._conn.execute(b'SETEX', key, seconds, value)
        return wait_ok(fut)

    def setnx(self, key, value):
        """Set the value of a key, only if the key does not exist."""
        fut = self._conn.execute(b'SETNX', key, value)
        return wait_convert(fut, bool)

    def setrange(self, key, offset, value):
        """Overwrite part of a string at key starting at the specified offset.

        :raises TypeError: if offset is not int
        :raises ValueError: if offset less then 0
        """
        if not isinstance(offset, int):
            raise TypeError("offset argument must be int")
        if offset < 0:
            raise ValueError("offset must be greater equal 0")
        return self._conn.execute(b'SETRANGE', key, offset, value)

    def strlen(self, key):
        """Get the length of the value stored in a key."""
        return self._conn.execute(b'STRLEN', key)
