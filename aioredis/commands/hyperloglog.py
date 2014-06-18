import asyncio


class HyperLogLogCommandsMixin:
    """hyperloglog commands mixin.

    For commands details see: http://redis.io/commands#hyperloglog
    """
    @asyncio.coroutine
    def pfadd(self, key, value, *values):
        """Adds the specified elements to the specified HyperLogLog."""
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'PFADD', key, value, *values))

    @asyncio.coroutine
    def pfcount(self, key, *keys):
        """Return the approximated cardinality of
        the set observed by the HyperLogLog at key."""
        return (yield from self._conn.execute(b'PFCOUNT', key, *keys))

    @asyncio.coroutine
    def pfmerge(self, destkey, sourcekey, *sourcekeys):
        """Merge N different HyperLogLogs into a single one."""
        if destkey is None:
            raise TypeError("key argument must not be None")

        return (yield from self._conn.execute(
                b'PFMERGE', destkey, sourcekey, *sourcekeys))
