import asyncio
from aioredis.errors import WrongArgumentError


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
    def bitcount(self, key, *pairs):
        """Count set bits in a string.
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'BITCOUNT', key, *pairs))

    @asyncio.coroutine
    def bitop(self, operation, destkey, key, *keys):
        """Perform bitwise operations between strings.
        """
        operations = {b'AND', b'OR', b'XOR', b'NOT'}
        operation = operation.upper().encode('utf-8')
        if operation not in operations:
            raise WrongArgumentError('operation must '
                                     'be on of: {}'.format(list(operations)))
        if operation == b'NOT' and len(keys) != 0:
            raise WrongArgumentError('NOT operation does not require '
                                     '*keys arguments')
        elif not (operation == b'NOT') and len(keys) == 0:
            raise WrongArgumentError('{} operation should have one or more '
                                     '*keys specified'.format(operation))
        return (yield from self._conn.execute(
            b'BITOP', operation, destkey, key, *keys))

    @asyncio.coroutine
    def bitpos(self, key, bit, start=None, end=None):
        """Find first bit set or clear in a string.
        """
        if bit not in (0, 1):
            raise WrongArgumentError('bit must be 0 or 1')
        args = []
        start is not None and args.append(start)
        if start is not None and end is not None:
            args.append(end)
        elif start is None and end is not None:
            raise WrongArgumentError("start arg not specified")
        return (yield from self._conn.execute(b'BITPOS', key, bit, *args))

    @asyncio.coroutine
    def decr(self, key):
        """Decrement the integer value of a key by one.
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'DECR', key))

    @asyncio.coroutine
    def decrby(self, key, decrement=1):
        """Decrement the integer value of a key by the given number.
        """
        if key is None:
            raise TypeError("key argument must not be None")
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
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'GETBIT', key, offset))

    @asyncio.coroutine
    def getrange(self, key, start, end):
        """Returns the substring of the string value stored at key,
        determined by the offsets start and end (both are inclusive).
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'GETRANGE', key, start, end))

    @asyncio.coroutine
    def getset(self, key, value):
        """Set the string value of a key and return its old value.
        """
        return (yield from self._conn.execute(b'GETSET', key, value))

    @asyncio.coroutine
    def incr(self, key):
        """Increment the integer value of a key by one.
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'INCR', key))

    @asyncio.coroutine
    def incrby(self, key, value):
        """Increment the integer value of a key by the given amount.
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'INCRBY', key, value))

    @asyncio.coroutine
    def incrbyfloat(self, key, value):
        """Increment the float value of a key by the given amount.
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'INCRBYFLOAT', key, value))

    @asyncio.coroutine
    def mget(self, key, *keys):
        """Get the values of all the given keys.
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'MGET', key, *keys))

    @asyncio.coroutine
    def mset(self, key, value, *pairs):
        """Set multiple keys to multiple values.
        """
        return (yield from self._conn.execute(b'MSET', key, value, *pairs))

    @asyncio.coroutine
    def msetnx(self, key, value, *pairs):
        """Set multiple keys to multiple values, only if none of the keys exist
        """
        return (yield from self._conn.execute(b'MSETNX', key, value, *pairs))

    @asyncio.coroutine
    def psetex(self, key, milliseconds, value):
        """Set the value and expiration in milliseconds of a key.
        """
        return (yield from self._conn.execute(
            b'PSETEX', key, milliseconds, value))

    @asyncio.coroutine
    def set(self, key, value, expire=None, pexpire=None,
            only_if_not_exists=False, only_if_exists=False):
        """Set the string value of a key.
        """
        args = []
        expire is not None and args.extend((b'EX', expire))
        pexpire is not None and args.extend((b'PX', pexpire))
        if only_if_not_exists and only_if_exists:
            raise WrongArgumentError('only_if_not_exists and only_if_exists '
                                     'cannot be true simultaneously')
        only_if_not_exists and args.append(b'NX')
        only_if_exists and args.append(b'XX')

        if key is None:
            raise TypeError('key argument must not be None')
        return (yield from self._conn.execute(b'SET', key, value, *args))

    @asyncio.coroutine
    def setbit(self, key, offset, value):
        """Sets or clears the bit at offset in the string value stored at key.
        """
        if key is None:
            raise TypeError('key argument must not be None')
        if value not in (0, 1):
            raise WrongArgumentError('value must be 0 or 1')
        return (yield from self._conn.execute(
            b'SETBIT', key, offset, value))

    @asyncio.coroutine
    def setex(self, key, seconds, value):
        """Set the value and expiration of a key.
        """
        return (yield from self._conn.execute(b'SETEX', key, seconds, value))

    @asyncio.coroutine
    def setnx(self, key, value):
        """Set the value of a key, only if the key does not exist.
        """
        return (yield from self._conn.execute(b'SETNX', key, value))

    @asyncio.coroutine
    def setrange(self, key, offset, value):
        """Overwrite part of a string at key starting at the specified offset.
        """
        return (yield from self._conn.execute(b'SETRANGE', key, offset, value))

    @asyncio.coroutine
    def strlen(self, key):
        """Get the length of the value stored in a key.
        """
        return (yield from self._conn.execute(b'STRLEN', key))
