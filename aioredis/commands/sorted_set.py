import asyncio
import math


class SortedSetCommandsMixin:
    """Sorted Sets commands mixin.

    For commands details see: http://redis.io/commands/#sorted_set
    """

    @asyncio.coroutine
    def zadd(self, key, score, member, *pairs):
        """Add one or more members to a sorted set or update its score.

        :raises TypeError: if key is None
        :raises TypeError: score not int or float
        :raises TypeError: length of pairs is not even number
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(score, (int, float)):
            raise TypeError("score argument must be int or float")
        if len(pairs) % 2 != 0:
            raise TypeError("length of pairs must be even number")

        scores = (item for i, item in enumerate(pairs) if i % 2 == 0)
        if any(not isinstance(s, (int, float)) for s in scores):
            raise TypeError("all scores must be int or float")
        return (yield from self._conn.execute(b'ZADD', key, score, member,
                                              *pairs))

    @asyncio.coroutine
    def zcard(self, key):
        """Get the number of members in a sorted set.

        :raises TypeError: if key is None
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'ZCARD', key))

    @asyncio.coroutine
    def zcount(self, key, min=float(b'-inf'), max=float(b'inf'),
               include_min=True, include_max=True):
        """Count the members in a sorted set with scores
        within the given values.

        :raises TypeError: if key is None
        :raises TypeError: min or max is not float or int
        :raises ValueError: if min grater then max
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(min, (int, float)):
            raise TypeError("min argument must be int or float")
        if not isinstance(max, (int, float)):
            raise TypeError("max argument must be int or float")
        if min > max:
            raise ValueError("min could not be grater then max")

        if not include_min and not math.isinf(min):
            min = ("(" + str(min)).encode('utf-8')
        if not include_max and not math.isinf(max):
            max = ("(" + str(max)).encode('utf-8')

        return (yield from self._conn.execute(b'ZCOUNT', key, min, max))

    @asyncio.coroutine
    def zincrby(self, key, increment, member):
        """Increment the score of a member in a sorted set.

        :raises TypeError: if key is None
        :raises TypeError: increment is not float or int
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(increment, (int, float)):
            raise TypeError("increment argument must be int or float")
        result = (yield from self._conn.execute(b'ZINCRBY', key,
                                                increment, member))
        return self._convert_to_int_or_float(result)

    @asyncio.coroutine
    def zinterstore(self, destkey, numkeys, key, *keys):  # TODO: weighs, etc
        """Intersect multiple sorted sets and store result in a new key."""
        raise NotImplementedError

    @asyncio.coroutine
    def zlexcount(self, key, min=b'-', max=b'+', include_min=True,
                  include_max=True):
        """Count the number of members in a sorted set between a given
        lexicographical range.

        :raises TypeError: if key is None
        :raises TypeError: if min is not bytes
        :raises TypeError: if max is not bytes
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(min, bytes):
            raise TypeError("min argument must be bytes")
        if not isinstance(max, bytes):
            raise TypeError("max argument must be bytes")
        if not min == b'-':
            min = (b'[' if include_min else b'(') + min
        if not max == b'+':
            max = (b'[' if include_max else b'(') + max
        return (yield from self._conn.execute(b'ZLEXCOUNT', key, min, max))

    @asyncio.coroutine
    def zrange(self, key, start=0, stop=-1, withscores=False):
        """Return a range of members in a sorted set, by index.

        :raises TypeError: if key is None
        :raises TypeError: if start is not int
        :raises TypeError: if stop is not int
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(start, int):
            raise TypeError("start argument must be int")
        if not isinstance(stop, int):
            raise TypeError("stop argument must be int")
        if withscores:
            args = [b'WITHSCORES']
        else:
            args = []
        result = (yield from self._conn.execute(
            b'ZRANGE', key, start, stop, *args))
        if withscores:
            f = lambda i, v:  self._convert_to_int_or_float(v) if i % 2 else v
            result = [f(i, r) for i, r in enumerate(result)]
        return result

    @asyncio.coroutine
    def zrangebylex(self, key, min=b'-', max=b'+', include_min=True,
                    include_max=True, offset=None, count=None):
        """Return a range of members in a sorted set, by lexicographical range.

        :raises TypeError: if key is None
        :raises TypeError: if min is not bytes
        :raises TypeError: if max is not bytes
        :raises TypeError: if both offset and count are not specified
        :raises TypeError: if offset is not bytes
        :raises TypeError: if count is not bytes

        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(min, bytes):
            raise TypeError("min argument must be bytes")
        if not isinstance(max, bytes):
            raise TypeError("max argument must be bytes")
        if not min == b'-':
            min = (b'[' if include_min else b'(') + min
        if not max == b'+':
            max = (b'[' if include_max else b'(') + max

        if (offset is not None and count is None) or \
                (count is not None and offset is None):
            raise TypeError("offset and count must both be specified")
        if offset is not None and not isinstance(offset, int):
            raise TypeError("offset argument must be int")
        if count is not None and not isinstance(count, int):
            raise TypeError("count argument must be int")

        args = []
        if offset is not None and count is not None:
            args.extend([b'LIMIT', offset, count])

        return (yield from self._conn.execute(
            b'ZRANGEBYLEX', key, min, max, *args))

    @asyncio.coroutine
    def zrangebyscore(self, key, min=float(b'-inf'), max=float(b'inf'),
                      include_min=True, include_max=True,
                      withscores=False, offset=None, count=None):
        """Return a range of memebers in a sorted set, by score.

        :raises TypeError: if key is None
        :raises TypeError: if min or max is not float or int
        :raises TypeError: if both offset and count are not specified
        :raises TypeError: if offset is not int
        :raises TypeError: if count is not int
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(min, (int, float)):
            raise TypeError("min argument must be int or float")
        if not isinstance(max, (int, float)):
            raise TypeError("max argument must be int or float")

        if (offset is not None and count is None) or \
                (count is not None and offset is None):
            raise TypeError("offset and count must both be specified")
        if offset is not None and not isinstance(offset, int):
            raise TypeError("offset argument must be int")
        if count is not None and not isinstance(count, int):
            raise TypeError("count argument must be int")

        if not include_min and not math.isinf(min):
            min = ("(" + str(min)).encode('utf-8')
        if not include_max and not math.isinf(max):
            max = ("(" + str(max)).encode('utf-8')

        args = []
        if withscores:
            args = [b'WITHSCORES']
        if offset is not None and count is not None:
            args.extend([b'LIMIT', offset, count])
        result = (yield from self._conn.execute(
            b'ZRANGEBYSCORE', key, min, max, *args))

        if withscores:
            f = lambda i, v:  self._convert_to_int_or_float(v) if i % 2 else v
            result = [f(i, r) for i, r in enumerate(result)]
        return result

    @asyncio.coroutine
    def zrank(self, key, member):
        """Determine the index of a member in a sorted set.

        :raises TypeError: if key is None
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'ZRANK', key, member))

    @asyncio.coroutine
    def zrem(self, key, member, *members):
        """Remove one or more members from a sorted set.

        :raises TypeError: if key is None
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'ZREM', key, member, *members))

    @asyncio.coroutine
    def zremrangebylex(self, key, min=b'-', max=b'+', include_min=True,
                       include_max=True,):
        """Remove all members in a sorted set between the given
        lexicographical range.

        :raises TypeError: if key is None
        :raises TypeError: if min is not bytes
        :raises TypeError: if max is not bytes
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(min, bytes):
            raise TypeError("min argument must be bytes")
        if not isinstance(max, bytes):
            raise TypeError("max argument must be bytes")
        if not min == b'-':
            min = (b'[' if include_min else b'(') + min
        if not max == b'+':
            max = (b'[' if include_max else b'(') + max
        return (yield from self._conn.execute(b'ZREMRANGEBYLEX',
                                              key, min, max))

    @asyncio.coroutine
    def zremrangebyrank(self, key, start, stop):
        """Remove all members in a sorted set within the given indexes.

        :raises TypeError: if key is None
        :raises TypeError: if start is not int
        :raises TypeError: if stop is not int
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(start, int):
            raise TypeError("start argument must be int")
        if not isinstance(stop, int):
            raise TypeError("stop argument must be int")
        return (yield from self._conn.execute(
            b'ZREMRANGEBYRANK', key, start, stop))

    @asyncio.coroutine
    def zremrangebyscore(self, key, min=float(b'-inf'), max=float(b'inf'),
                         include_min=True, include_max=True):
        """Remove all members in a sorted set within the given scores.

        :raises TypeError: if key is None
        :raises TypeError: if min is not int or float
        :raises TypeError: if max is not int or float
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(min, (int, float)):
            raise TypeError("min argument must be int or float")
        if not isinstance(max, (int, float)):
            raise TypeError("max argument must be int or float")

        if not include_min and not math.isinf(min):
            min = ("(" + str(min)).encode('utf-8')
        if not include_max and not math.isinf(max):
            max = ("(" + str(max)).encode('utf-8')

        return (yield from self._conn.execute(
            b'ZREMRANGEBYSCORE', key, min, max))

    @asyncio.coroutine
    def zrevrange(self, key, start, stop, *, withscores=False):
        """Return a range of members in a sorted set, by index,
        with scores ordered from high to low.
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(start, int):
            raise TypeError("start argument must be int")
        if not isinstance(stop, int):
            raise TypeError("stop argument must be int")
        if withscores:
            args = [b'WITHSCORES']
        else:
            args = []
        return (yield from self._conn.execute(
            b'ZREVRANGE', key, start, stop, *args))

    @asyncio.coroutine
    def zrevrangebyscore(self, key, max, min, *, withscores=False, limit=None):
        """Return a range of members in a sorted set, by score,
        with scores ordered from high to low.
        """
        raise NotImplementedError

    @asyncio.coroutine
    def zrevrank(self, key, member):
        """Determine the index of a member in a sorted set, with
        scores ordered from high to low.

        :raises TypeError: if key is None
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'ZREVRANK', key, member))

    @asyncio.coroutine
    def zscore(self, key, member):
        """Get the score associated with the given emmber in a sorted set.
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'ZSCORE', key, member))

    @asyncio.coroutine
    def zunionstore(self, destkey, numkeys, key, *keys):  # TODO: weights, etc
        """Add multiple sorted sets and store result in a new key."""
        raise NotImplementedError

    @asyncio.coroutine
    def zscan(self, key, cursor, match=None, count=None):
        """Incrementally iterate sorted sets elements and associated scores."""
        if key is None:
            raise TypeError("key argument must not be None")
        args = []
        if match is not None:
            args += [b'MATCH', match]
        if count is not None:
            args += [b'COUNT', count]
        cursor, items = yield from self._conn.execute(
            b'ZSCAN', key, cursor, *args)
        return int(cursor), items

    def _convert_to_int_or_float(self, raw_value):
        assert isinstance(raw_value, bytes), 'raw_value must be bytes'
        value_str = raw_value.decode('utf-8')
        try:
            value = int(value_str)
        except ValueError:
            value = float(value_str)
        return value
