from itertools import chain

from aioredis.util import (
    wait_ok,
    wait_convert,
    wait_make_dict,
    _NOTSET,
    PY_35,
    )

if PY_35:
    from aioredis.util import _ScanIterPairs


class HashCommandsMixin:
    """Hash commands mixin.

    For commands details see: http://redis.io/commands#hash
    """

    def hdel(self, key, field, *fields):
        """Delete one or more hash fields."""
        return self.execute(b'HDEL', key, field, *fields)

    def hexists(self, key, field):
        """Determine if hash field exists."""
        fut = self.execute(b'HEXISTS', key, field)
        return wait_convert(fut, bool)

    def hget(self, key, field, *, encoding=_NOTSET):
        """Get the value of a hash field."""
        return self.execute(b'HGET', key, field, encoding=encoding)

    def hgetall(self, key, *, encoding=_NOTSET):
        """Get all the fields and values in a hash."""
        fut = self.execute(b'HGETALL', key, encoding=encoding)
        return wait_make_dict(fut)

    def hincrby(self, key, field, increment=1):
        """Increment the integer value of a hash field by the given number."""
        return self.execute(b'HINCRBY', key, field, increment)

    def hincrbyfloat(self, key, field, increment=1.0):
        """Increment the float value of a hash field by the given number."""
        fut = self.execute(b'HINCRBYFLOAT', key, field, increment)
        return wait_convert(fut, float)

    def hkeys(self, key, *, encoding=_NOTSET):
        """Get all the fields in a hash."""
        return self.execute(b'HKEYS', key, encoding=encoding)

    def hlen(self, key):
        """Get the number of fields in a hash."""
        return self.execute(b'HLEN', key)

    def hmget(self, key, field, *fields, encoding=_NOTSET):
        """Get the values of all the given fields."""
        return self.execute(b'HMGET', key, field, *fields, encoding=encoding)

    def hmset(self, key, field, value, *pairs):
        """Set multiple hash fields to multiple values."""
        if len(pairs) % 2 != 0:
            raise TypeError("length of pairs must be even number")
        return wait_ok(self.execute(b'HMSET', key, field, value, *pairs))

    def hmset_dict(self, key, *args, **kwargs):
        """Set multiple hash fields to multiple values.

        dict can be passed as first positional argument:

        >>> await redis.hmset_dict(
        ...     'key', {'field1': 'value1', 'field2': 'value2'})

        or keyword arguments can be used:

        >>> await redis.hmset_dict(
        ...     'key', field1='value1', field2='value2')

        or dict argument can be mixed with kwargs:

        >>> await redis.hmset_dict(
        ...     'key', {'field1': 'value1'}, field2='value2')

        .. note:: ``dict`` and ``kwargs`` not get mixed into single dictionary,
           if both specified and both have same key(s) -- ``kwargs`` will win:

           >>> await redis.hmset_dict('key', {'foo': 'bar'}, foo='baz')
           >>> await redis.hget('key', 'foo', encoding='utf-8')
           'baz'

        """
        if not args and not kwargs:
            raise TypeError("args or kwargs must be specified")
        pairs = ()
        if len(args) > 1:
            raise TypeError("single positional argument allowed")
        elif len(args) == 1:
            if not isinstance(args[0], dict):
                raise TypeError("args[0] must be dict")
            elif not args[0] and not kwargs:
                raise ValueError("args[0] is empty dict")
            pairs = chain.from_iterable(args[0].items())
        kwargs_pairs = chain.from_iterable(kwargs.items())
        return wait_ok(self.execute(
            b'HMSET', key, *chain(pairs, kwargs_pairs)))

    def hset(self, key, field, value):
        """Set the string value of a hash field."""
        return self.execute(b'HSET', key, field, value)

    def hsetnx(self, key, field, value):
        """Set the value of a hash field, only if the field does not exist."""
        return self.execute(b'HSETNX', key, field, value)

    def hvals(self, key, *, encoding=_NOTSET):
        """Get all the values in a hash."""
        return self.execute(b'HVALS', key, encoding=encoding)

    def hscan(self, key, cursor=0, match=None, count=None):
        """Incrementally iterate hash fields and associated values."""
        args = [key, cursor]
        match is not None and args.extend([b'MATCH', match])
        count is not None and args.extend([b'COUNT', count])
        fut = self.execute(b'HSCAN', *args)
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
        return self.execute(b'HSTRLEN', key, field)
