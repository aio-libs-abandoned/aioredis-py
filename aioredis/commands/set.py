from aioredis.util import wait_convert, _NOTSET, PY_35


if PY_35:
    from aioredis.util import _ScanIter


class SetCommandsMixin:
    """Set commands mixin.

    For commands details see: http://redis.io/commands#set
    """

    def sadd(self, key, member, *members):
        """Add one or more members to a set."""
        return self._conn.execute(b'SADD', key, member, *members)

    def scard(self, key):
        """Get the number of members in a set."""
        return self._conn.execute(b'SCARD', key)

    def sdiff(self, key, *keys):
        """Subtract multiple sets."""
        return self._conn.execute(b'SDIFF', key, *keys)

    def sdiffstore(self, destkey, key, *keys):
        """Subtract multiple sets and store the resulting set in a key."""
        return self._conn.execute(b'SDIFFSTORE', destkey, key, *keys)

    def sinter(self, key, *keys):
        """Intersect multiple sets."""
        return self._conn.execute(b'SINTER', key, *keys)

    def sinterstore(self, destkey, key, *keys):
        """Intersect multiple sets and store the resulting set in a key."""
        return self._conn.execute(b'SINTERSTORE', destkey, key, *keys)

    def sismember(self, key, member):
        """Determine if a given value is a member of a set."""
        return self._conn.execute(b'SISMEMBER', key, member)

    def smembers(self, key, *, encoding=_NOTSET):
        """Get all the members in a set."""
        return self._conn.execute(b'SMEMBERS', key, encoding=encoding)

    def smove(self, sourcekey, destkey, member):
        """Move a member from one set to another."""
        return self._conn.execute(b'SMOVE', sourcekey, destkey, member)

    def spop(self, key, *, encoding=_NOTSET):
        """Remove and return a random member from a set."""
        return self._conn.execute(b'SPOP', key, encoding=encoding)

    def srandmember(self, key, count=None, *, encoding=_NOTSET):
        """Get one or multiple random members from a set."""
        args = [key]
        count is not None and args.append(count)
        return self._conn.execute(b'SRANDMEMBER', *args, encoding=encoding)

    def srem(self, key, member, *members):
        """Remove one or more members from a set."""
        return self._conn.execute(b'SREM', key, member, *members)

    def sunion(self, key, *keys):
        """Add multiple sets."""
        return self._conn.execute(b'SUNION', key, *keys)

    def sunionstore(self, destkey, key, *keys):
        """Add multiple sets and store the resulting set in a key."""
        return self._conn.execute(b'SUNIONSTORE', destkey, key, *keys)

    def sscan(self, key, cursor=0, match=None, count=None):
        """Incrementally iterate Set elements."""
        tokens = [key, cursor]
        match is not None and tokens.extend([b'MATCH', match])
        count is not None and tokens.extend([b'COUNT', count])
        fut = self._conn.execute(b'SSCAN', *tokens)
        return wait_convert(fut, lambda obj: (int(obj[0]), obj[1]))

    if PY_35:
        def isscan(self, key, *, match=None, count=None):
            """Incrementally iterate set elements using async for.

            Usage example:

            >>> async for val in redis.isscan(key, match='something*'):
            ...     print('Matched:', val)

            """
            return _ScanIter(lambda cur: self.sscan(key, cur,
                                                    match=match,
                                                    count=count))
