

class HyperLogLogCommandsMixin:
    """HyperLogLog commands mixin.

    For commands details see: http://redis.io/commands#hyperloglog
    """

    def pfadd(self, key, value, *values):
        """Adds the specified elements to the specified HyperLogLog.

        :raises TypeError: if key is None
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return self._conn.execute(b'PFADD', key, value, *values)

    def pfcount(self, key, *keys):
        """Return the approximated cardinality of
        the set(s) observed by the HyperLogLog at key(s).

        :raises TypeError: if any of arguments is None
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if None in set(keys):
            raise TypeError("keys must not contain None")
        return self._conn.execute(b'PFCOUNT', key, *keys)

    def pfmerge(self, destkey, sourcekey, *sourcekeys):
        """Merge N different HyperLogLogs into a single one.

        :raises TypeError: if any of arguments is None
        """
        if destkey is None:
            raise TypeError("destkey argument must not be None")
        if sourcekey is None:
            raise TypeError("sourcekey argument must not be None")
        if None in set(sourcekeys):
            raise TypeError("sourcekeys must not contain None")
        return self._conn.execute(b'PFMERGE', destkey, sourcekey, *sourcekeys)
