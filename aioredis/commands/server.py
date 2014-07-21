import asyncio


class ServerCommandsMixin:
    """Server commands mixin.

    For commands details see: http://redis.io/commands/#server
    """

    @asyncio.coroutine
    def bgrewriteaof(self):
        """Asynchronously rewrite the append-only file."""
        raise NotImplementedError

    @asyncio.coroutine
    def bgsave(self):
        """Asynchronously save the dataset to disk."""
        raise NotImplementedError

    @asyncio.coroutine
    def client_kill(self):
        """Kill the connection of a client."""
        raise NotImplementedError

    @asyncio.coroutine
    def client_list(self):
        """Get the list of client connections."""
        raise NotImplementedError

    @asyncio.coroutine
    def client_getname(self):
        """Get the current connection name."""
        return (yield from self._conn.execute(b'CLIENT', b'GETNAME'))

    @asyncio.coroutine
    def client_pause(self, timeout):
        """Stop processing commands from clients for some time."""
        raise NotImplementedError

    @asyncio.coroutine
    def client_setname(self, name):
        """Set the current connection name."""
        if name is None:
            raise TypeError("name argument must not be None")
        return (yield from self._conn.execute(b'CLIENT', b'SETNAME', name))

    @asyncio.coroutine
    def config_get(self, parameter):
        """Get the value of a configuration parameter."""
        raise NotImplementedError

    @asyncio.coroutine
    def config_rewrite(self):
        """Rewrite the configuration file with the in memory configuration."""
        raise NotImplementedError

    @asyncio.coroutine
    def config_set(self, parameter, value):
        """Set a configuration parameter to the given value."""
        raise NotImplementedError

    @asyncio.coroutine
    def config_resetstat(self):
        """Reset the stats returned by INFO."""
        raise NotImplementedError

    @asyncio.coroutine
    def dbsize(self):
        """Return the number of keys in the selected database."""
        return (yield from self._conn.execute(b'DBSIZE'))

    @asyncio.coroutine
    def debug_object(self, key):
        """Get debugging information about a key."""
        if key is None:
            raise TypeError("key argument must not be None")
        return (yield from self._conn.execute(b'DEBUG', b'OBJECT', key))

    @asyncio.coroutine
    def debug_segfault(self, key):
        """Make the server crash."""
        return (yield from self._conn.execute(b'DEBUG', 'SEGFAULT'))

    @asyncio.coroutine
    def flushall(self):
        """Remove all keys from all databases."""
        res = yield from self._conn.execute(b'FLUSHALL')
        return res == b'OK'

    @asyncio.coroutine
    def flushdb(self):
        """Remove all keys from the current database."""
        res = yield from self._conn.execute('FLUSHDB')
        return res == b'OK'

    @asyncio.coroutine
    def info(self, section):
        """Get information and statistics about the server."""
        # TODO: check section
        return (yield from self._conn.execute(b'INFO', section))

    @asyncio.coroutine
    def lastsave(self):
        """Get the UNIX time stamp of the last successful save to disk."""
        raise NotImplementedError

    @asyncio.coroutine
    def monitor(self):
        raise NotImplementedError

    @asyncio.coroutine
    def role(self):
        """Return the role of the instance in the context of replication."""
        return (yield from self._conn.execute(b'ROLE'))

    @asyncio.coroutine
    def save(self):
        """Synchronously save the dataset to disk."""
        return (yield from self._conn.execute(b'SAVE'))

    @asyncio.coroutine
    def shutdown(self):
        """Synchronously save the dataset to disk and then
        shut down the server.
        """
        raise NotImplementedError

    @asyncio.coroutine
    def slaveof(self):
        """Make the server a slave of another instance,
        or promote it as master.
        """
        raise NotImplementedError

    @asyncio.coroutine
    def slowlog(self):
        """Manages the Redis slow queries log."""
        raise NotImplementedError

    @asyncio.coroutine
    def sync(self):
        """Redis-server internal command used for replication."""
        return (yield from self._conn.execute(b'SYNC'))

    @asyncio.coroutine
    def time(self):
        """Return current server time."""
        res = yield from self._conn.execute(b'TIME')
        return float(b'.'.join(res))
