from aioredis.util import wait_convert, wait_ok, _NOTSET, _ScanIter


class GenericCommandsMixin:
    """Generic commands mixin.

    For commands details see: http://redis.io/commands/#generic
    """

    def delete(self, key, *keys):
        """Delete a key."""
        fut = self.execute(b'DEL', key, *keys)
        return wait_convert(fut, int)

    def dump(self, key):
        """Dump a key."""
        return self.execute(b'DUMP', key)

    def exists(self, key, *keys):
        """Check if key(s) exists.

        .. versionchanged:: v0.2.9
           Accept multiple keys; **return** type **changed** from bool to int.
        """
        return self.execute(b'EXISTS', key, *keys)

    def expire(self, key, timeout):
        """Set a timeout on key.

        if timeout is float it will be multiplied by 1000
        coerced to int and passed to `pexpire` method.

        Otherwise raises TypeError if timeout argument is not int.
        """
        if isinstance(timeout, float):
            return self.pexpire(key, int(timeout * 1000))
        if not isinstance(timeout, int):
            raise TypeError(
                "timeout argument must be int, not {!r}".format(timeout))
        fut = self.execute(b'EXPIRE', key, timeout)
        return wait_convert(fut, bool)

    def expireat(self, key, timestamp):
        """Set expire timestamp on a key.

        if timeout is float it will be multiplied by 1000
        coerced to int and passed to `pexpireat` method.

        Otherwise raises TypeError if timestamp argument is not int.
        """
        if isinstance(timestamp, float):
            return self.pexpireat(key, int(timestamp * 1000))
        if not isinstance(timestamp, int):
            raise TypeError("timestamp argument must be int, not {!r}"
                            .format(timestamp))
        fut = self.execute(b'EXPIREAT', key, timestamp)
        return wait_convert(fut, bool)

    def keys(self, pattern, *, encoding=_NOTSET):
        """Returns all keys matching pattern."""
        return self.execute(b'KEYS', pattern, encoding=encoding)

    def migrate(self, host, port, key, dest_db, timeout, *,
                copy=False, replace=False):
        """Atomically transfer a key from a Redis instance to another one."""
        if not isinstance(host, str):
            raise TypeError("host argument must be str")
        if not isinstance(timeout, int):
            raise TypeError("timeout argument must be int")
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
        fut = self.execute(b'MIGRATE', host, port,
                           key, dest_db, timeout, *flags)
        return wait_ok(fut)

    def migrate_keys(self, host, port, keys, dest_db, timeout, *,
                     copy=False, replace=False):
        """Atomically transfer keys from one Redis instance to another one.

        Keys argument must be list/tuple of keys to migrate.
        """
        if not isinstance(host, str):
            raise TypeError("host argument must be str")
        if not isinstance(timeout, int):
            raise TypeError("timeout argument must be int")
        if not isinstance(dest_db, int):
            raise TypeError("dest_db argument must be int")
        if not isinstance(keys, (list, tuple)):
            raise TypeError("keys argument must be list or tuple")
        if not host:
            raise ValueError("Got empty host")
        if dest_db < 0:
            raise ValueError("dest_db must be greater equal 0")
        if timeout < 0:
            raise ValueError("timeout must be greater equal 0")
        if not keys:
            raise ValueError("keys must not be empty")

        flags = []
        if copy:
            flags.append(b'COPY')
        if replace:
            flags.append(b'REPLACE')
        flags.append(b'KEYS')
        flags.extend(keys)
        fut = self.execute(b'MIGRATE', host, port,
                           "", dest_db, timeout, *flags)
        return wait_ok(fut)

    def move(self, key, db):
        """Move key from currently selected database to specified destination.

        :raises TypeError: if db is not int
        :raises ValueError: if db is less than 0
        """
        if not isinstance(db, int):
            raise TypeError("db argument must be int, not {!r}".format(db))
        if db < 0:
            raise ValueError("db argument must be not less than 0, {!r}"
                             .format(db))
        fut = self.execute(b'MOVE', key, db)
        return wait_convert(fut, bool)

    def object_refcount(self, key):
        """Returns the number of references of the value associated
        with the specified key (OBJECT REFCOUNT).
        """
        return self.execute(b'OBJECT', b'REFCOUNT', key)

    def object_encoding(self, key):
        """Returns the kind of internal representation used in order
        to store the value associated with a key (OBJECT ENCODING).
        """
        # TODO: set default encoding to 'utf-8'
        return self.execute(b'OBJECT', b'ENCODING', key)

    def object_idletime(self, key):
        """Returns the number of seconds since the object is not requested
        by read or write operations (OBJECT IDLETIME).
        """
        return self.execute(b'OBJECT', b'IDLETIME', key)

    def persist(self, key):
        """Remove the existing timeout on key."""
        fut = self.execute(b'PERSIST', key)
        return wait_convert(fut, bool)

    def pexpire(self, key, timeout):
        """Set a milliseconds timeout on key.

        :raises TypeError: if timeout is not int
        """
        if not isinstance(timeout, int):
            raise TypeError("timeout argument must be int, not {!r}"
                            .format(timeout))
        fut = self.execute(b'PEXPIRE', key, timeout)
        return wait_convert(fut, bool)

    def pexpireat(self, key, timestamp):
        """Set expire timestamp on key, timestamp in milliseconds.

        :raises TypeError: if timeout is not int
        """
        if not isinstance(timestamp, int):
            raise TypeError("timestamp argument must be int, not {!r}"
                            .format(timestamp))
        fut = self.execute(b'PEXPIREAT', key, timestamp)
        return wait_convert(fut, bool)

    def pttl(self, key):
        """Returns time-to-live for a key, in milliseconds.

        Special return values (starting with Redis 2.8):

        * command returns -2 if the key does not exist.
        * command returns -1 if the key exists but has no associated expire.
        """
        # TODO: maybe convert negative values to:
        #       -2 to None  - no key
        #       -1 to False - no expire
        return self.execute(b'PTTL', key)

    def randomkey(self, *, encoding=_NOTSET):
        """Return a random key from the currently selected database."""
        return self.execute(b'RANDOMKEY', encoding=encoding)

    def rename(self, key, newkey):
        """Renames key to newkey.

        :raises ValueError: if key == newkey
        """
        if key == newkey:
            raise ValueError("key and newkey are the same")
        fut = self.execute(b'RENAME', key, newkey)
        return wait_ok(fut)

    def renamenx(self, key, newkey):
        """Renames key to newkey only if newkey does not exist.

        :raises ValueError: if key == newkey
        """
        if key == newkey:
            raise ValueError("key and newkey are the same")
        fut = self.execute(b'RENAMENX', key, newkey)
        return wait_convert(fut, bool)

    def restore(self, key, ttl, value):
        """Creates a key associated with a value that is obtained via DUMP."""
        return self.execute(b'RESTORE', key, ttl, value)

    def scan(self, cursor=0, match=None, count=None):
        """Incrementally iterate the keys space.

        Usage example:

        >>> match = 'something*'
        >>> cur = b'0'
        >>> while cur:
        ...     cur, keys = await redis.scan(cur, match=match)
        ...     for key in keys:
        ...         print('Matched:', key)

        """
        args = []
        if match is not None:
            args += [b'MATCH', match]
        if count is not None:
            args += [b'COUNT', count]
        fut = self.execute(b'SCAN', cursor, *args)
        return wait_convert(fut, lambda o: (int(o[0]), o[1]))

    def iscan(self, *, match=None, count=None):
        """Incrementally iterate the keys space using async for.

        Usage example:

        >>> async for key in redis.iscan(match='something*'):
        ...     print('Matched:', key)

        """
        return _ScanIter(lambda cur: self.scan(cur,
                                               match=match, count=count))

    def sort(self, key, *get_patterns,
             by=None, offset=None, count=None,
             asc=None, alpha=False, store=None):
        """Sort the elements in a list, set or sorted set."""
        args = []
        if by is not None:
            args += [b'BY', by]
        if offset is not None and count is not None:
            args += [b'LIMIT', offset, count]
        if get_patterns:
            args += sum(([b'GET', pattern] for pattern in get_patterns), [])
        if asc is not None:
            args += [asc is True and b'ASC' or b'DESC']
        if alpha:
            args += [b'ALPHA']
        if store is not None:
            args += [b'STORE', store]
        return self.execute(b'SORT', key, *args)

    def touch(self, key, *keys):
        """Alters the last access time of a key(s).

        Returns the number of keys that were touched.
        """
        return self.execute(b'TOUCH', key, *keys)

    def ttl(self, key):
        """Returns time-to-live for a key, in seconds.

        Special return values (starting with Redis 2.8):
        * command returns -2 if the key does not exist.
        * command returns -1 if the key exists but has no associated expire.
        """
        # TODO: maybe convert negative values to:
        #       -2 to None  - no key
        #       -1 to False - no expire
        return self.execute(b'TTL', key)

    def type(self, key):
        """Returns the string representation of the value's type stored at key.
        """
        # NOTE: for non-existent keys TYPE returns b'none'
        return self.execute(b'TYPE', key)

    def unlink(self, key, *keys):
        """Delete a key asynchronously in another thread."""
        return wait_convert(self.execute(b'UNLINK', key, *keys), int)

    def wait(self, numslaves, timeout):
        """Wait for the synchronous replication of all the write
        commands sent in the context of the current connection.
        """
        return self.execute(b'WAIT', numslaves, timeout)
