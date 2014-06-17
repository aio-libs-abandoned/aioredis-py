import asyncio


class HyperLogLogCommandsMixin:
    """hyperloglog commands mixin.

    For commands details see: http://redis.io/commands#hyperloglog
    """
    @asyncio.coroutine
    def pfadd(self, key, *values):
        """Adds the specified elements to the specified HyperLogLog."""
        if key is None:
            raise TypeError("key argument must not be None")
        if not values:
            raise TypeError("specify one or more values")
        return (yield from self._conn.execute(b'PFADD', key, *values))

    @asyncio.coroutine
    def pfcount(self, *keys):
        """Return the approximated cardinality of
        the set observed by the HyperLogLog at key."""
        if not keys:
            raise TypeError("specify one or more keys")
        return (yield from self._conn.execute(b'PFCOUNT', *keys))

    @asyncio.coroutine
    def pfmerge(self, destkey, *sourcekeys):
        """Merge N different HyperLogLogs into a single one."""
        if destkey is None:
            raise TypeError("key argument must not be None")
        if not sourcekeys:
            raise TypeError("sourcekeys does not specified")
        return (
            yield from self._conn.execute(b'PFMERGE', destkey, *sourcekeys)
        )
