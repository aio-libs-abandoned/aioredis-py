import asyncio


class SetCommandsMixin:
    """Set commands mixin.

    For commands details see: http://redis.io/commands#set
    """

    @asyncio.coroutine
    def sadd(self, key, member, *members):
        """Add one or more members to a set.

        :raises TypeError: if key is None
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'SADD', key, member, *members))

    @asyncio.coroutine
    def scard(self, key):
        """Get the number of members in a set.

        :raises TypeError: if key is None
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'SCARD', key))

    @asyncio.coroutine
    def sdiff(self, key, *keys):
        """Subtract multiple sets.

        :raises TypeError: if any of arguments is None
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if None in set(keys):
            raise TypeError("keys must not contain None")
        return (yield from self._conn.execute(b'SDIFF', key, *keys))

    @asyncio.coroutine
    def sdiffstore(self, destkey, key, *keys):
        """Subtract multiple sets and store the resulting set in a key.

        :raises TypeError: if any of arguments is None
        """
        if destkey is None:
            raise TypeError("destkey argument must not be None")
        if key is None:
            raise TypeError("key argument must not be None")
        if None in set(keys):
            raise TypeError("keys must not contain None")
        return (yield from self._conn.execute(
            b'SDIFFSTORE', destkey, key, *keys))

    @asyncio.coroutine
    def sinter(self, key, *keys):
        """Intersect multiple sets.

        :raises TypeError: if any of arguments is None
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if None in set(keys):
            raise TypeError("keys must not contain None")
        return (yield from self._conn.execute(b'SINTER', key, *keys))

    @asyncio.coroutine
    def sinterstore(self, destkey, key, *keys):
        """Intersect multiple sets and store the resulting set in a key.

        :raises TypeError: if any of arguments is None
        """
        if destkey is None:
            raise TypeError("destkey argument must not be None")
        if key is None:
            raise TypeError("key argument must not be None")
        if None in set(keys):
            raise TypeError("keys must not contain None")
        return (yield from self._conn.execute(
            b'SINTERSTORE', destkey, key, *keys))

    @asyncio.coroutine
    def sismember(self, key, member):
        """Determine if a given value is a member of a set.

        :raises TypeError: if key is None
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'SISMEMBER', key, member))

    @asyncio.coroutine
    def smembers(self, key):
        """Get all the members in a set.

        :raises TypeError: if key is None
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'SMEMBERS', key))

    @asyncio.coroutine
    def smove(self, sourcekey, destkey, member):
        """Move a member from one set to another.

        :raises TypeError: if sourcekey or destkey is None
        """
        if sourcekey is None:
            raise TypeError("sourcekey argument must not be None")
        if destkey is None:
            raise TypeError("destkey argument must not be None")
        return (yield from self._conn.execute(
            b'SMOVE', sourcekey, destkey, member))

    @asyncio.coroutine
    def spop(self, key):
        """Remove and return a random member from a set.

        :raises TypeError: if key is None
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'SPOP', key))

    @asyncio.coroutine
    def srandmember(self, key, count=None):
        """Get one or multiple random members from a set.

        :raises TypeError: if key is None
        """
        if key is None:
            raise TypeError("key argument must not be None")
        args = [key]
        count is not None and args.append(count)
        return (yield from self._conn.execute(b'SRANDMEMBER', *args))

    @asyncio.coroutine
    def srem(self, key, member, *members):
        """Remove one or more members from a set.

        :raises TypeError: if key is None
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'SREM', key, member, *members))

    @asyncio.coroutine
    def sunion(self, key, *keys):
        """Add multiple sets.

        :raises TypeError: if any of arguments is None
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if None in set(keys):
            raise TypeError("keys must not contain None")
        return (yield from self._conn.execute(b'SUNION', key, *keys))

    @asyncio.coroutine
    def sunionstore(self, destkey, key, *keys):
        """Add multiple sets and store the resulting set in a key.

        :raises TypeError: if any of arguments is None
        """
        if destkey is None:
            raise TypeError("destkey argument must not be None")
        if key is None:
            raise TypeError("key argument must not be None")
        if None in set(keys):
            raise TypeError("keys must not contain None")
        return (yield from self._conn.execute(
            b'SUNIONSTORE', destkey, key, *keys))

    @asyncio.coroutine
    def sscan(self, key, cursor=0, match=None, count=None):
        """Incrementally iterate Set elements.

        :raises TypeError: if key is None
        """
        if key is None:
            raise TypeError("key argument must not be None")

        tokens = [key, cursor]
        match is not None and tokens.extend([b'MATCH', match])
        count is not None and tokens.extend([b'COUNT', count])
        cursor, value = yield from self._conn.execute(b'SSCAN', *tokens)
        return int(cursor), value
