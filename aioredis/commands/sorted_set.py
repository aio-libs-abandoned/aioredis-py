import asyncio


class SortedSetCommandsMixin:
    """Sorted Sets commands mixin.

    For commands details see: http://redis.io/commands/#sorted_set
    """

    @asyncio.coroutine
    def zadd(self, key, score, member, *pairs):
        """Add one or more members to a sorted set or update its score."""
        raise NotImplementedError

    @asyncio.coroutine
    def zcard(self, key):
        """Get the number of members in a sorted set."""
        raise NotImplementedError

    @asyncio.coroutine
    def zcount(self, key, min, max):
        """Count the members in a sorted set with scores
        within the given values.
        """
        raise NotImplementedError

    @asyncio.coroutine
    def zincrby(self, key, increment, member):
        """Increment the score of a member in a sorted set."""
        raise NotImplementedError

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
    def zrange(self, key, start, stop, *, withscores=False):
        """Return a range of members in a sorted set, by index."""
        raise NotImplementedError

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
        raise NotImplementedError

    @asyncio.coroutine
    def zrem(self, key, member, *members):
        """Remove one or more members from a sorted set."""
        raise NotImplementedError

    @asyncio.coroutine
    def zremrangebylex(self, key, min, max):
        """Remove all members in a sorted set between the given
        lexicographical range.
        """
        raise NotImplementedError

    @asyncio.coroutine
    def zremrangebyrank(self, key, start, stop):
        """Remove all members in a sorted set within the given indexes."""
        raise NotImplementedError

    @asyncio.coroutine
    def zremrangebyscore(self, key, min, max):
        """Remove all members in a sorted set within the given scores."""
        raise NotImplementedError

    @asyncio.coroutine
    def zrevrange(self, key, start, stop, *, withscores=False):
        """Return a range of members in a sorted set, by index,
        with scores ordered from high to low.
        """
        raise NotImplementedError

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
        raise NotImplementedError

    @asyncio.coroutine
    def zscore(self, key, member):
        """Get the score associated with the given emmber in a sorted set.
        """
        raise NotImplementedError

    @asyncio.coroutine
    def zunionstore(self, destkey, numkeys, key, *keys):  # TODO: weights, etc
        """Add multiple sorted sets and store result in a new key."""
        raise NotImplementedError

    @asyncio.coroutine
    def zscan(self, key, cursor, match=None, count=None):
        """Incrementally iterate sorted sets elements and associated scores."""
        raise NotImplementedError
