from aioredis.util import wait_ok, wait_convert, _NOTSET


class ServerCommandsMixin:
    """Server commands mixin.

    For commands details see: http://redis.io/commands/#server
    """

    def bgrewriteaof(self):
        """Asynchronously rewrite the append-only file."""
        raise NotImplementedError

    def bgsave(self):
        """Asynchronously save the dataset to disk."""
        raise NotImplementedError

    def client_kill(self):
        """Kill the connection of a client."""
        raise NotImplementedError

    def client_list(self):
        """Get the list of client connections."""
        raise NotImplementedError

    def client_getname(self, encoding=_NOTSET):
        """Get the current connection name."""
        return self._conn.execute(b'CLIENT', b'GETNAME', encoding=encoding)

    def client_pause(self, timeout):
        """Stop processing commands from clients for some time."""
        raise NotImplementedError

    def client_setname(self, name):
        """Set the current connection name."""
        if name is None:
            raise TypeError("name argument must not be None")
        fut = self._conn.execute(b'CLIENT', b'SETNAME', name)
        return wait_ok(fut)

    def config_get(self, parameter):
        """Get the value of a configuration parameter."""
        raise NotImplementedError

    def config_rewrite(self):
        """Rewrite the configuration file with the in memory configuration."""
        raise NotImplementedError

    def config_set(self, parameter, value):
        """Set a configuration parameter to the given value."""
        raise NotImplementedError

    def config_resetstat(self):
        """Reset the stats returned by INFO."""
        raise NotImplementedError

    def dbsize(self):
        """Return the number of keys in the selected database."""
        return self._conn.execute(b'DBSIZE')

    def debug_object(self, key):
        """Get debugging information about a key."""
        if key is None:
            raise TypeError("key argument must not be None")
        return self._conn.execute(b'DEBUG', b'OBJECT', key)

    def debug_segfault(self, key):
        """Make the server crash."""
        return self._conn.execute(b'DEBUG', 'SEGFAULT')

    def flushall(self):
        """Remove all keys from all databases."""
        fut = self._conn.execute(b'FLUSHALL')
        return wait_ok(fut)

    def flushdb(self):
        """Remove all keys from the current database."""
        fut = self._conn.execute('FLUSHDB')
        return wait_ok(fut)

    def info(self, section):
        """Get information and statistics about the server."""
        # TODO: check section
        return self._conn.execute(b'INFO', section)

    def lastsave(self):
        """Get the UNIX time stamp of the last successful save to disk."""
        raise NotImplementedError

    def monitor(self):
        raise NotImplementedError

    def role(self):
        """Return the role of the instance in the context of replication."""
        return self._conn.execute(b'ROLE')

    def save(self):
        """Synchronously save the dataset to disk."""
        return self._conn.execute(b'SAVE')

    def shutdown(self):
        """Synchronously save the dataset to disk and then
        shut down the server.
        """
        raise NotImplementedError

    def slaveof(self):
        """Make the server a slave of another instance,
        or promote it as master.
        """
        raise NotImplementedError

    def slowlog(self):
        """Manages the Redis slow queries log."""
        raise NotImplementedError

    def sync(self):
        """Redis-server internal command used for replication."""
        return self._conn.execute(b'SYNC')

    def time(self):
        """Return current server time."""
        fut = self._conn.execute(b'TIME')
        return wait_convert(fut, lambda obj: float(b'.'.join(obj)))
