from aioredis.util import wait_ok


class HyperLogLogCommandsMixin:
    """HyperLogLog commands mixin.

    For commands details see: http://redis.io/commands#hyperloglog
    """

    def pfadd(self, key, value, *values):
        """Adds the specified elements to the specified HyperLogLog."""
        return self._conn.execute(b'PFADD', key, value, *values)

    def pfcount(self, key, *keys):
        """Return the approximated cardinality of
        the set(s) observed by the HyperLogLog at key(s).
        """
        return self._conn.execute(b'PFCOUNT', key, *keys)

    def pfmerge(self, destkey, sourcekey, *sourcekeys):
        """Merge N different HyperLogLogs into a single one."""
        fut = self._conn.execute(b'PFMERGE', destkey, sourcekey, *sourcekeys)
        return wait_ok(fut)
