from aioredis.util import wait_convert, wait_make_dict, _NOTSET, PY_35

if PY_35:
    from aioredis.util import _ScanIterPairs


class HashCommandsMixin:
    """Hash commands mixin.

    For commands details see: http://redis.io/commands#hash
    """

    def hdel(self, key, field, *fields):
        """Delete one or more hash fields."""
        return self._conn.execute(b'HDEL', key, field, *fields)

    def hexists(self, key, field):
        """Determine if hash field exists."""
        fut = self._conn.execute(b'HEXISTS', key, field)
        return wait_convert(fut, bool)

    def hget(self, key, field, *, encoding=_NOTSET):
        """Get the value of a hash field."""
        return self._conn.execute(b'HGET', key, field, encoding=encoding)

    def hgetall(self, key, *, encoding=_NOTSET):
        """Get all the fields and values in a hash."""
        fut = self._conn.execute(b'HGETALL', key, encoding=encoding)
        return wait_make_dict(fut)

    def hincrby(self, key, field, increment=1):
        """Increment the integer value of a hash field by the given number."""
        return self._conn.execute(b'HINCRBY', key, field, increment)

    def hincrbyfloat(self, key, field, increment=1.0):
        """Increment the float value of a hash field by the given number."""
        fut = self._conn.execute(b'HINCRBYFLOAT', key, field, increment)
        return wait_convert(fut, float)

    def hkeys(self, key, *, encoding=_NOTSET):
        """Get all the fields in a hash."""
        return self._conn.execute(b'HKEYS', key, encoding=encoding)

    def hlen(self, key):
        """Get the number of fields in a hash."""
        return self._conn.execute(b'HLEN', key)

    def hmget(self, key, field, *fields, encoding=_NOTSET):
        """Get the values of all the given fields."""
        return self._conn.execute(b'HMGET', key, field, *fields,
                                  encoding=encoding)

    # TODO: replace args with dict_or_pairs
    def hmset(self, key, field, value, *pairs):
        """Set multiple hash fields to multiple values."""
        if len(pairs) % 2 != 0:
            raise TypeError("length of pairs must be even number")
        return self._conn.execute(b'HMSET', key, field, value, *pairs)

    def hset(self, key, field, value):
        """Set the string value of a hash field."""
        return self._conn.execute(b'HSET', key, field, value)

    def hsetnx(self, key, field, value):
        """Set the value of a hash field, only if the field does not exist."""
        return self._conn.execute(b'HSETNX', key, field, value)

    def hvals(self, key, *, encoding=_NOTSET):
        """Get all the values in a hash."""
        return self._conn.execute(b'HVALS', key, encoding=encoding)

    def hscan(self, key, cursor=0, match=None, count=None):
        """Incrementally iterate hash fields and associated values."""
        args = [key, cursor]
        match is not None and args.extend([b'MATCH', match])
        count is not None and args.extend([b'COUNT', count])
        fut = self._conn.execute(b'HSCAN', *args)
        return wait_convert(fut, lambda obj: (int(obj[0]), obj[1]))

    if PY_35:
        def ihscan(self, key, *, match=None, count=None):
            """Incrementally iterate sorted set items using async for.

            Usage example:

            >>> async for name, val in redis.ihscan(key, match='something*'):
            ...     print('Matched:', name, '->', val)

            """
            return _ScanIterPairs(lambda cur: self.hscan(key, cur,
                                                         match=match,
                                                         count=count))

    def hstrlen(self, key, field):
        """Get the length of the value of a hash field."""
        return self._conn.execute(b'HSTRLEN', key, field)
