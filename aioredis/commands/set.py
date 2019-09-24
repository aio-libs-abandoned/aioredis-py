from aioredis.util import wait_convert, _NOTSET, _ScanIter


class SetCommandsMixin:
    """Set commands mixin.

    For commands details see: http://redis.io/commands#set
    """

    def sadd(self, key, member, *members):
        """Add one or more members to a set."""
        return self.execute(b'SADD', key, member, *members)

    def scard(self, key):
        """Get the number of members in a set."""
        return self.execute(b'SCARD', key)

    def sdiff(self, key, *keys):
        """Subtract multiple sets."""
        return self.execute(b'SDIFF', key, *keys)

    def sdiffstore(self, destkey, key, *keys):
        """Subtract multiple sets and store the resulting set in a key."""
        return self.execute(b'SDIFFSTORE', destkey, key, *keys)

    def sinter(self, key, *keys):
        """Intersect multiple sets."""
        return self.execute(b'SINTER', key, *keys)

    def sinterstore(self, destkey, key, *keys):
        """Intersect multiple sets and store the resulting set in a key."""
        return self.execute(b'SINTERSTORE', destkey, key, *keys)

    def sismember(self, key, member):
        """Determine if a given value is a member of a set."""
        return self.execute(b'SISMEMBER', key, member)

    def smembers(self, key, *, encoding=_NOTSET):
        """Get all the members in a set."""
        return self.execute(b'SMEMBERS', key, encoding=encoding)

    def smove(self, sourcekey, destkey, member):
        """Move a member from one set to another."""
        return self.execute(b'SMOVE', sourcekey, destkey, member)

    def spop(self, key, count=None, *, encoding=_NOTSET):
        """Remove and return one or multiple random members from a set."""
        args = [key]
        if count is not None:
            args.append(count)
        return self.execute(b'SPOP', *args, encoding=encoding)

    def srandmember(self, key, count=None, *, encoding=_NOTSET):
        """Get one or multiple random members from a set."""
        args = [key]
        count is not None and args.append(count)
        return self.execute(b'SRANDMEMBER', *args, encoding=encoding)

    def srem(self, key, member, *members):
        """Remove one or more members from a set."""
        return self.execute(b'SREM', key, member, *members)

    def sunion(self, key, *keys):
        """Add multiple sets."""
        return self.execute(b'SUNION', key, *keys)

    def sunionstore(self, destkey, key, *keys):
        """Add multiple sets and store the resulting set in a key."""
        return self.execute(b'SUNIONSTORE', destkey, key, *keys)

    def sscan(self, key, cursor=0, match=None, count=None):
        """Incrementally iterate Set elements."""
        tokens = [key, cursor]
        match is not None and tokens.extend([b'MATCH', match])
        count is not None and tokens.extend([b'COUNT', count])
        fut = self.execute(b'SSCAN', *tokens)
        return wait_convert(fut, lambda obj: (int(obj[0]), obj[1]))

    def isscan(self, key, *, match=None, count=None):
        """Incrementally iterate set elements using async for.

        Usage example:

        >>> async for val in redis.isscan(key, match='something*'):
        ...     print('Matched:', val)

        """
        return _ScanIter(lambda cur: self.sscan(key, cur,
                                                match=match,
                                                count=count))
