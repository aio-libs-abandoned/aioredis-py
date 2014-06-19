import asyncio


class GenericCommandsMixin:
    """Generic commands mixin.

    For commands details see: http://redis.io/commands/#generic
    """

    @asyncio.coroutine
    def delete(self, key, *keys):
        """Delete a key.
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if any(k is None for k in keys):
            raise TypeError("keys must not be None")
        ret = yield from self._conn.execute(b'DEL', key, *keys)
        return int(ret)

    @asyncio.coroutine
    def dump(self, key):
        """Dump a key.
        """
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'DUMP', key))

    @asyncio.coroutine
    def exists(self, key):
        """Check if key exists.
        """
        if key is None:
            raise TypeError("key argument must not be None")
        ret = yield from self._conn.execute(b'EXISTS', key)
        return bool(ret)

    @asyncio.coroutine
    def expire(self, key, timeout):
        """Set a timeout on key.

        if timeout is float it will be multiplyed by 1000
        coerced to int and passed to `pexpire` method.

        Otherwise raises TypeError if timeout argument is not int
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if isinstance(timeout, float):
            ret = yield from self.pexpire(key, int(timeout * 1000))
            return ret
        if not isinstance(timeout, int):
            raise TypeError("timeout argument must be int, not {!r}"
                            .format(timeout))
        ret = yield from self._conn.execute(b'EXPIRE', key, timeout)
        return bool(ret)

    @asyncio.coroutine
    def expireat(self, key, timestamp):
        """Set expire timestamp on key.

        if timeout is float it will be multiplyed by 1000
        coerced to int and passed to `pexpire` method.

        Otherwise raises TypeError if timestamp argument is not int
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if isinstance(timestamp, float):
            ret = yield from self.pexpireat(key, int(timestamp * 1000))
            return ret
        if not isinstance(timestamp, int):
            raise TypeError("timestamp argument must be int, not {!r}"
                            .format(timestamp))
        ret = yield from self._conn.execute(b'EXPIREAT', key, timestamp)
        return bool(ret)

    @asyncio.coroutine
    def keys(self, pattern):
        """Returns all keys matching pattern.
        """
        if pattern is None:
            raise TypeError("pattern argument must not be None")
        return (yield from self._conn.execute(b'KEYS', pattern))

    @asyncio.coroutine
    def migrate(self, host, port, key, dest_db, timeout,
                copy=False, replace=False):
        """Atomically transfer a key from a Redis instance to another one.
        """
        if not isinstance(host, str):
            raise TypeError("host argument must be str")
        if not isinstance(timeout, int):
            raise TypeError("timeout argument must be int")
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(dest_db, int):
            raise TypeError("dest_db argument must be int")
        if not host:
            raise ValueError("Got empty host")
        if dest_db < 0:
            raise ValueError("dest_db must be greater equal 0")
        if timeout < 0:
            raise ValueError("timeout must be greater equal 0")

        flags = []
        if copy:
            flags.append(b'COPY')
        if replace:
            flags.append(b'REPLACE')
        res = yield from self._conn.execute(b'MIGRATE', host, port,
                                            key, dest_db, timeout, *flags)
        return res == b'OK'

    @asyncio.coroutine
    def move(self, key, db):
        """Move key from currently selected database to specified destination.

        Raises TypeError if db is not int and ValueError if db is <= 0
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(db, int):
            raise TypeError("db argument must be int, not {!r}".format(db))
        if db < 0:
            raise ValueError("db argument must be not less then 0, {!r}"
                             .format(db))
        ret = yield from self._conn.execute(b'MOVE', key, db)
        return bool(ret)

    @asyncio.coroutine
    def object(self):
        pass
        # TODO: implement

    @asyncio.coroutine
    def persist(self, key):
        """Remove the existing timeout on key.
        """
        if key is None:
            raise TypeError("key argument must not be None")
        ret = yield from self._conn.execute(b'PERSIST', key)
        return bool(ret)

    @asyncio.coroutine
    def pexpire(self, key, timeout):
        """Set a milliseconds timeout on key.
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(timeout, int):
            raise TypeError("timeout argument must be int, not {!r}"
                            .format(timeout))
        ret = yield from self._conn.execute(b'PEXPIRE', key, timeout)
        return bool(ret)

    @asyncio.coroutine
    def pexpireat(self, key, timestamp):
        """Set expire timestamp on key, timestamp in milliseconds.
        """
        if key is None:
            raise TypeError("key argument must not be None")
        if not isinstance(timestamp, int):
            raise TypeError("timestamp argument must be int, not {!r}"
                            .format(timestamp))
        ret = yield from self._conn.execute(b'PEXPIREAT', key, timestamp)
        return bool(ret)

    @asyncio.coroutine
    def pttl(self, key):
        """Returns time-to-live for a key, in milliseconds.

        Special return values (starting with Redis 2.8):
        * command returns -2 if the key does not exist.
        * command returns -1 if the key exists but has no associated expire.
        """
        ret = yield from self._conn.execute(b'PTTL', key)
        # TODO: maybe convert negative values to:
        #       -2 to None  - no key
        #       -1 to False - no expire
        return ret

    @asyncio.coroutine
    def randomkey(self):
        """Return a random key from the currently selected database.
        """
        return (yield from self._conn.execute(b'RANDOMKEY'))

    @asyncio.coroutine
    def rename(self, key, newkey):
        """Renames key to newkey.

        Raises ValueError if key == newkey
        """
        if key == newkey:
            raise ValueError("key and newkey are the same")
        ret = yield from self._conn.execute(b'RENAME', key, newkey)
        return ret

    @asyncio.coroutine
    def renamenx(self, key, newkey):
        """Renames key to newkey only if newkey does not exist.

        Raises ValueError if key == newkey
        """
        if key == newkey:
            raise ValueError("key and newkey are the same")
        ret = yield from self._conn.execute(b'RENAMENX', key, newkey)
        return ret

    @asyncio.coroutine
    def restore(self, key, ttl, value):
        """Creates a key associated with a value that is obtained via DUMP.
        """
        ret = yield from self._conn.execute(b'RESTORE', key, ttl, value)
        return ret

    @asyncio.coroutine
    def scan(self):
        pass
        # TODO: implement

    @asyncio.coroutine
    def sort(self):
        pass
        # TODO: implement

    @asyncio.coroutine
    def ttl(self, key):
        """Returns time-to-live for a key, in seconds.

        Special return values (starting with Redis 2.8):
        * command returns -2 if the key does not exist.
        * command returns -1 if the key exists but has no associated expire.
        """
        ret = yield from self._conn.execute(b'TTL', key)
        # TODO: maybe convert negative values to:
        #       -2 to None  - no key
        #       -1 to False - no expire
        return ret

    @asyncio.coroutine
    def type(self, key):
        """Returns the string representation of the type of value stored at key
        """
        return (yield from self._conn.execute(b'TYPE', key))
