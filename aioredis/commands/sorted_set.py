import ast
import asyncio
import math


class SortedSetCommandsMixin:
    """Sorted Sets commands mixin.

    For commands details see: http://redis.io/commands/#sorted_set
    """

    @asyncio.coroutine
    def zadd(self, key, score, member, *pairs):
        """Add one or more members to a sorted set or update its score.

        :raises TypeError: 1) if key is None 2) score not int or float
        3) length of pairs is not even number
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

        :raises TypeError: 1) if key is None 2) min or max is not float or int
        :raises ValueError: if min grater then max
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(min, (int, float)):
            raise TypeError("min argument must be int or float")
        if not isinstance(max, (int, float)):
            raise TypeError("min argument must be int or float")
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

        :raises TypeError: 1) if key is None 2) increment is not float or int
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(increment, (int, float)):
            raise TypeError("increment argument must be int or float")
        result = (yield from self._conn.execute(b'ZINCRBY', key,
                                                increment, member))
        return ast.literal_eval(result.decode('utf-8'))

    @asyncio.coroutine
    def zinterstore(self, destkey, numkeys, key, *keys):  # TODO: weighs, etc
        """Intersect multiple sorted sets and store result in a new key."""
        raise NotImplementedError

    @asyncio.coroutine
    def zlexcount(self, key, min, max):
        """Count the number of members in a sorted set between a given
        lexicographical range.
        """
        raise NotImplementedError

    @asyncio.coroutine
    def zrange(self, key, start=0, stop=-1, withscores=False):
        """Return a range of members in a sorted set, by index."""
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
            b'ZRANGE', key, start, stop, *args))

    @asyncio.coroutine
    def zrangebylex(self, key, min, max, *, limit=None):
        """Return a range of members in a sorted set, by lexicographical range.
        """
        raise NotImplementedError

    @asyncio.coroutine
    def zrangebyscore(self, key, min, max, *, withscores=False, limit=None):
        """Return a range of memebers in a sorted set, by score."""
        raise NotImplementedError

    @asyncio.coroutine
    def zrank(self, key, member):
        """Determine the index of a member in a sorted set."""
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'ZRANK', key, member))

    @asyncio.coroutine
    def zrem(self, key, member, *members):
        """Remove one or more members from a sorted set."""
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'ZREM', key, member, *members))

    @asyncio.coroutine
    def zremrangebylex(self, key, min, max):
        """Remove all members in a sorted set between the given
        lexicographical range.
        """
        raise NotImplementedError

    @asyncio.coroutine
    def zremrangebyrank(self, key, start, stop):
        """Remove all members in a sorted set within the given indexes."""
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(start, int):
            raise TypeError("start argument must be int")
        if not isinstance(stop, int):
            raise TypeError("stop argument must be int")
        return (yield from self._conn.execute(
            b'ZREMRANGEBYRANK', key, start, stop))

    @asyncio.coroutine
    def zremrangebyscore(self, key, min, max):
        """Remove all members in a sorted set within the given scores."""
        raise NotImplementedError

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
