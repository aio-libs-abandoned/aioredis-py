import asyncio
import itertools
from .encoders import nativestr


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
    def hget(self, key, field, encoder=nativestr):
        """Get the value of a hash field"""
        result = yield from self._conn.execute(b'HGET', key, field)
        return encoder(result) if encoder else result

    @asyncio.coroutine
    def hgetall(self, key, encoder=nativestr):
        """Get all the fields and values in a hash"""
        results = yield from self._conn.execute(b'HGETALL', key)
        if encoder:
            results = [encoder(r) for r in results]
        return results

    @asyncio.coroutine
    def hincrby(self, key, field, increment=1):
        """Increment the integer value of a hash field by the given number"""
        return (yield from self._conn.execute(
            b'HINCRBY', key, field, increment))

    @asyncio.coroutine
    def hincrbyfloat(self, key, field, increment=1.0, encoder=float):
        """Increment the integer value of a hash field by the given number"""
        result = yield from self._conn.execute(
            b'HINCRBYFLOAT', key, field, increment)
        return encoder(result) if encoder and result else result

    @asyncio.coroutine
    def hkeys(self, key, encoder=nativestr):
        """Get all the fields in a hash"""
        results = yield from self._conn.execute(b'HKEYS', key)
        if encoder:
            results = [encoder(r) for r in results]
        return results

    @asyncio.coroutine
    def hlen(self, key):
        """Returns the number of fields contained in the hash
        stored at key."""
        return (yield from self._conn.execute(b'HLEN', key))

    @asyncio.coroutine
    def hmget(self, key, field, *fields, encoder=nativestr):
        """Returns the values associated with the specified fields in
        the hash stored at key."""
        results = yield from self._conn.execute(b'HMGET', key, field, *fields)
        if encoder:
            results = [encoder(r) if r else None for r in results]
        return results

    @asyncio.coroutine
    def hmset(self, key, mapping):
        """Set field to value within hash ``key`` for each corresponding
        field and value from the ``mapping`` dict."""
        if not mapping:
            raise ValueError("'hmset' with 'mapping' of length 0")
        items = list(itertools.chain(*mapping.items()))
        return (yield from self._conn.execute(b'HMSET', key, *items))

    @asyncio.coroutine
    def hset(self, key, field, value):
        """Set the string value of a hash field"""
        return (yield from self._conn.execute(b'HSET', key, field, value))

    @asyncio.coroutine
    def hsetnx(self, key, field, value):
        """Set the value of a hash field, only if the field does not exist"""
        return (yield from self._conn.execute(b'HSETNX', key, field, value))

    @asyncio.coroutine
    def hvals(self, key, encoder=nativestr):
        """Get all the values in a hash"""
        results = yield from self._conn.execute(b'HVALS', key)
        if encoder:
            results = [encoder(r) for r in results]
        return results

    @asyncio.coroutine
    def hscan(self, key, cursor=0, match=None, count=None):
        """Incrementally iterate hash fields and associated values"""
        tokens = [key, cursor]
        match is not None and tokens.extend([b'MATCH', match])
        count is not None and tokens.extend([b'COUNT', count])
        return (yield from self._conn.execute(b'HSCAN', *tokens))
