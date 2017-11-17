from aioredis.util import wait_convert, _NOTSET, _ScanIter


class SortedSetCommandsMixin:
    """Sorted Sets commands mixin.

    For commands details see: http://redis.io/commands/#sorted_set
    """

    ZSET_EXCLUDE_MIN = 'ZSET_EXCLUDE_MIN'
    ZSET_EXCLUDE_MAX = 'ZSET_EXCLUDE_MAX'
    ZSET_EXCLUDE_BOTH = 'ZSET_EXCLUDE_BOTH'

    ZSET_AGGREGATE_SUM = 'ZSET_AGGREGATE_SUM'
    ZSET_AGGREGATE_MIN = 'ZSET_AGGREGATE_MIN'
    ZSET_AGGREGATE_MAX = 'ZSET_AGGREGATE_MAX'

    ZSET_IF_NOT_EXIST = 'ZSET_IF_NOT_EXIST'  # NX
    ZSET_IF_EXIST = 'ZSET_IF_EXIST'  # XX

    def zadd(self, key, score, member, *pairs, exist=None):
        """Add one or more members to a sorted set or update its score.

        :raises TypeError: score not int or float
        :raises TypeError: length of pairs is not even number
        """
        if not isinstance(score, (int, float)):
            raise TypeError("score argument must be int or float")
        if len(pairs) % 2 != 0:
            raise TypeError("length of pairs must be even number")

        scores = (item for i, item in enumerate(pairs) if i % 2 == 0)
        if any(not isinstance(s, (int, float)) for s in scores):
            raise TypeError("all scores must be int or float")

        args = []
        if exist is self.ZSET_IF_EXIST:
            args.append(b'XX')
        elif exist is self.ZSET_IF_NOT_EXIST:
            args.append(b'NX')

        args.extend([score, member])
        if pairs:
            args.extend(pairs)
        return self.execute(b'ZADD', key, *args)

    def zcard(self, key):
        """Get the number of members in a sorted set."""
        return self.execute(b'ZCARD', key)

    def zcount(self, key, min=float('-inf'), max=float('inf'),
               *, exclude=None):
        """Count the members in a sorted set with scores
        within the given values.

        :raises TypeError: min or max is not float or int
        :raises ValueError: if min grater then max
        """
        if not isinstance(min, (int, float)):
            raise TypeError("min argument must be int or float")
        if not isinstance(max, (int, float)):
            raise TypeError("max argument must be int or float")
        if min > max:
            raise ValueError("min could not be grater then max")
        return self.execute(b'ZCOUNT', key,
                            *_encode_min_max(exclude, min, max))

    def zincrby(self, key, increment, member):
        """Increment the score of a member in a sorted set.

        :raises TypeError: increment is not float or int
        """
        if not isinstance(increment, (int, float)):
            raise TypeError("increment argument must be int or float")
        fut = self.execute(b'ZINCRBY', key, increment, member)
        return wait_convert(fut, int_or_float)

    def zinterstore(self, destkey, key, *keys,
                    with_weights=False, aggregate=None):
        """Intersect multiple sorted sets and store result in a new key.

        :param bool with_weights: when set to true each key must be a tuple
                                  in form of (key, weight)
        """
        keys = (key,) + keys
        numkeys = len(keys)
        args = []
        if with_weights:
            assert all(isinstance(val, (list, tuple)) for val in keys), (
                "All key arguments must be (key, weight) tuples")
            weights = ['WEIGHTS']
            for key, weight in keys:
                args.append(key)
                weights.append(weight)
            args.extend(weights)
        else:
            args.extend(keys)

        if aggregate is self.ZSET_AGGREGATE_SUM:
            args.extend(('AGGREGATE', 'SUM'))
        elif aggregate is self.ZSET_AGGREGATE_MAX:
            args.extend(('AGGREGATE', 'MAX'))
        elif aggregate is self.ZSET_AGGREGATE_MIN:
            args.extend(('AGGREGATE', 'MIN'))
        fut = self.execute(b'ZINTERSTORE', destkey, numkeys, *args)
        return fut

    def zlexcount(self, key, min=b'-', max=b'+', include_min=True,
                  include_max=True):
        """Count the number of members in a sorted set between a given
        lexicographical range.

        :raises TypeError: if min is not bytes
        :raises TypeError: if max is not bytes
        """
        if not isinstance(min, bytes):  # FIXME
            raise TypeError("min argument must be bytes")
        if not isinstance(max, bytes):  # FIXME     Why only bytes?
            raise TypeError("max argument must be bytes")
        if not min == b'-':
            min = (b'[' if include_min else b'(') + min
        if not max == b'+':
            max = (b'[' if include_max else b'(') + max
        return self.execute(b'ZLEXCOUNT', key, min, max)

    def zrange(self, key, start=0, stop=-1, withscores=False,
               encoding=_NOTSET):
        """Return a range of members in a sorted set, by index.

        :raises TypeError: if start is not int
        :raises TypeError: if stop is not int
        """
        if not isinstance(start, int):
            raise TypeError("start argument must be int")
        if not isinstance(stop, int):
            raise TypeError("stop argument must be int")
        if withscores:
            args = [b'WITHSCORES']
        else:
            args = []
        fut = self.execute(b'ZRANGE', key, start, stop, *args,
                           encoding=encoding)
        if withscores:
            return wait_convert(fut, pairs_int_or_float)
        return fut

    def zrangebylex(self, key, min=b'-', max=b'+', include_min=True,
                    include_max=True, offset=None, count=None,
                    encoding=_NOTSET):
        """Return a range of members in a sorted set, by lexicographical range.

        :raises TypeError: if min is not bytes
        :raises TypeError: if max is not bytes
        :raises TypeError: if both offset and count are not specified
        :raises TypeError: if offset is not bytes
        :raises TypeError: if count is not bytes
        """
        if not isinstance(min, bytes):  # FIXME
            raise TypeError("min argument must be bytes")
        if not isinstance(max, bytes):  # FIXME
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

        return self.execute(b'ZRANGEBYLEX', key, min, max, *args,
                            encoding=encoding)

    def zrangebyscore(self, key, min=float('-inf'), max=float('inf'),
                      withscores=False, offset=None, count=None,
                      *, exclude=None, encoding=_NOTSET):
        """Return a range of members in a sorted set, by score.

        :raises TypeError: if min or max is not float or int
        :raises TypeError: if both offset and count are not specified
        :raises TypeError: if offset is not int
        :raises TypeError: if count is not int
        """
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

        min, max = _encode_min_max(exclude, min, max)

        args = []
        if withscores:
            args = [b'WITHSCORES']
        if offset is not None and count is not None:
            args.extend([b'LIMIT', offset, count])
        fut = self.execute(b'ZRANGEBYSCORE', key, min, max, *args,
                           encoding=encoding)
        if withscores:
            return wait_convert(fut, pairs_int_or_float)
        return fut

    def zrank(self, key, member):
        """Determine the index of a member in a sorted set."""
        return self.execute(b'ZRANK', key, member)

    def zrem(self, key, member, *members):
        """Remove one or more members from a sorted set."""
        return self.execute(b'ZREM', key, member, *members)

    def zremrangebylex(self, key, min=b'-', max=b'+',
                       include_min=True, include_max=True):
        """Remove all members in a sorted set between the given
        lexicographical range.

        :raises TypeError: if min is not bytes
        :raises TypeError: if max is not bytes
        """
        if not isinstance(min, bytes):  # FIXME
            raise TypeError("min argument must be bytes")
        if not isinstance(max, bytes):  # FIXME
            raise TypeError("max argument must be bytes")
        if not min == b'-':
            min = (b'[' if include_min else b'(') + min
        if not max == b'+':
            max = (b'[' if include_max else b'(') + max
        return self.execute(b'ZREMRANGEBYLEX', key, min, max)

    def zremrangebyrank(self, key, start, stop):
        """Remove all members in a sorted set within the given indexes.

        :raises TypeError: if start is not int
        :raises TypeError: if stop is not int
        """
        if not isinstance(start, int):
            raise TypeError("start argument must be int")
        if not isinstance(stop, int):
            raise TypeError("stop argument must be int")
        return self.execute(b'ZREMRANGEBYRANK', key, start, stop)

    def zremrangebyscore(self, key, min=float('-inf'), max=float('inf'),
                         *, exclude=None):
        """Remove all members in a sorted set within the given scores.

        :raises TypeError: if min or max is not int or float
        """
        if not isinstance(min, (int, float)):
            raise TypeError("min argument must be int or float")
        if not isinstance(max, (int, float)):
            raise TypeError("max argument must be int or float")

        min, max = _encode_min_max(exclude, min, max)
        return self.execute(b'ZREMRANGEBYSCORE', key, min, max)

    def zrevrange(self, key, start, stop, withscores=False, encoding=_NOTSET):
        """Return a range of members in a sorted set, by index,
        with scores ordered from high to low.

        :raises TypeError: if start or stop is not int
        """
        if not isinstance(start, int):
            raise TypeError("start argument must be int")
        if not isinstance(stop, int):
            raise TypeError("stop argument must be int")
        if withscores:
            args = [b'WITHSCORES']
        else:
            args = []
        fut = self.execute(b'ZREVRANGE', key, start, stop, *args,
                           encoding=encoding)
        if withscores:
            return wait_convert(fut, pairs_int_or_float)
        return fut

    def zrevrangebyscore(self, key, max=float('inf'), min=float('-inf'),
                         *, exclude=None, withscores=False,
                         offset=None, count=None, encoding=_NOTSET):
        """Return a range of members in a sorted set, by score,
        with scores ordered from high to low.

        :raises TypeError: if min or max is not float or int
        :raises TypeError: if both offset and count are not specified
        :raises TypeError: if offset is not int
        :raises TypeError: if count is not int
        """
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

        min, max = _encode_min_max(exclude, min, max)

        args = []
        if withscores:
            args = [b'WITHSCORES']
        if offset is not None and count is not None:
            args.extend([b'LIMIT', offset, count])
        fut = self.execute(b'ZREVRANGEBYSCORE', key, max, min, *args,
                           encoding=encoding)
        if withscores:
            return wait_convert(fut, pairs_int_or_float)
        return fut

    def zrevrangebylex(self, key, min=b'-', max=b'+', include_min=True,
                       include_max=True, offset=None, count=None,
                       encoding=_NOTSET):
        """Return a range of members in a sorted set, by lexicographical range
        from high to low.

        :raises TypeError: if min is not bytes
        :raises TypeError: if max is not bytes
        :raises TypeError: if both offset and count are not specified
        :raises TypeError: if offset is not bytes
        :raises TypeError: if count is not bytes
        """
        if not isinstance(min, bytes):  # FIXME
            raise TypeError("min argument must be bytes")
        if not isinstance(max, bytes):  # FIXME
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

        return self.execute(b'ZREVRANGEBYLEX', key, max, min, *args,
                            encoding=encoding)

    def zrevrank(self, key, member):
        """Determine the index of a member in a sorted set, with
        scores ordered from high to low.
        """
        return self.execute(b'ZREVRANK', key, member)

    def zscore(self, key, member):
        """Get the score associated with the given member in a sorted set."""
        fut = self.execute(b'ZSCORE', key, member)
        return wait_convert(fut, optional_int_or_float)

    def zunionstore(self, destkey, key, *keys,
                    with_weights=False, aggregate=None):
        """Add multiple sorted sets and store result in a new key."""
        keys = (key,) + keys
        numkeys = len(keys)
        args = []
        if with_weights:
            assert all(isinstance(val, (list, tuple)) for val in keys), (
                "All key arguments must be (key, weight) tuples")
            weights = ['WEIGHTS']
            for key, weight in keys:
                args.append(key)
                weights.append(weight)
            args.extend(weights)
        else:
            args.extend(keys)

        if aggregate is self.ZSET_AGGREGATE_SUM:
            args.extend(('AGGREGATE', 'SUM'))
        elif aggregate is self.ZSET_AGGREGATE_MAX:
            args.extend(('AGGREGATE', 'MAX'))
        elif aggregate is self.ZSET_AGGREGATE_MIN:
            args.extend(('AGGREGATE', 'MIN'))
        fut = self.execute(b'ZUNIONSTORE', destkey, numkeys, *args)
        return fut

    def zscan(self, key, cursor=0, match=None, count=None):
        """Incrementally iterate sorted sets elements and associated scores."""
        args = []
        if match is not None:
            args += [b'MATCH', match]
        if count is not None:
            args += [b'COUNT', count]
        fut = self.execute(b'ZSCAN', key, cursor, *args)

        def _converter(obj):
            return (int(obj[0]), pairs_int_or_float(obj[1]))

        return wait_convert(fut, _converter)

    def izscan(self, key, *, match=None, count=None):
        """Incrementally iterate sorted set items using async for.

        Usage example:

        >>> async for val, score in redis.izscan(key, match='something*'):
        ...     print('Matched:', val, ':', score)

        """
        return _ScanIter(lambda cur: self.zscan(key, cur,
                                                match=match,
                                                count=count))


def _encode_min_max(flag, min, max):
    if flag is SortedSetCommandsMixin.ZSET_EXCLUDE_MIN:
        return '({}'.format(min), max
    elif flag is SortedSetCommandsMixin.ZSET_EXCLUDE_MAX:
        return min, '({}'.format(max)
    elif flag is SortedSetCommandsMixin.ZSET_EXCLUDE_BOTH:
        return '({}'.format(min), '({}'.format(max)
    return min, max


def int_or_float(value):
    assert isinstance(value, (str, bytes)), 'raw_value must be bytes'
    try:
        return int(value)
    except ValueError:
        return float(value)


def optional_int_or_float(value):
    if value is None:
        return value
    return int_or_float(value)


def pairs_int_or_float(value):
    it = iter(value)
    return [(val, int_or_float(score))
            for val, score in zip(it, it)]
