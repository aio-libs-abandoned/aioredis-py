from aioredis.util import wait_convert, _NOTSET


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

    # TODO: add encoding param
    def hgetall(self, key):
        """Get all the fields and values in a hash."""
        fut = self._conn.execute(b'HGETALL', key)
        return wait_convert(fut, to_dict)

    def hincrby(self, key, field, increment=1):
        """Increment the integer value of a hash field by the given number."""
        return self._conn.execute(b'HINCRBY', key, field, increment)

    def hincrbyfloat(self, key, field, increment=1.0):
        """Increment the float value of a hash field by the given number."""
        fut = self._conn.execute(b'HINCRBYFLOAT', key, field, increment)
        return wait_convert(fut, float)

    # TODO: add encoding param
    def hkeys(self, key):
        """Get all the fields in a hash."""
        return self._conn.execute(b'HKEYS', key)

    def hlen(self, key):
        """Get the number of fields in a hash."""
        return self._conn.execute(b'HLEN', key)

    def hmget(self, key, field, *fields):
        """Get the values of all the given fields."""
        return self._conn.execute(b'HMGET', key, field, *fields)

    def hmset(self, key, field, value, *pairs):
        """Set multiple hash fields to multiple values."""
        return self._conn.execute(b'HMSET', key, field, value, *pairs)

    def hset(self, key, field, value):
        """Set the string value of a hash field."""
        return self._conn.execute(b'HSET', key, field, value)

    def hsetnx(self, key, field, value):
        """Set the value of a hash field, only if the field does not exist."""
        return self._conn.execute(b'HSETNX', key, field, value)

    def hvals(self, key):
        """Get all the values in a hash."""
        return self._conn.execute(b'HVALS', key)

    def hscan(self, key, cursor=0, match=None, count=None):
        """Incrementally iterate hash fields and associated values."""
        args = [key, cursor]
        match is not None and args.extend([b'MATCH', match])
        count is not None and args.extend([b'COUNT', count])
        fut = self._conn.execute(b'HSCAN', *args)
        return wait_convert(fut, lambda obj: (int(obj[0]), obj[1]))


def to_dict(value):
    it = iter(value)
    return dict(zip(it, it))
