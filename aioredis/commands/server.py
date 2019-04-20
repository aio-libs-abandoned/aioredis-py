from collections import namedtuple

from aioredis.util import wait_ok, wait_convert, wait_make_dict, _NOTSET


class ServerCommandsMixin:
    """Server commands mixin.

    For commands details see: http://redis.io/commands/#server
    """

    SHUTDOWN_SAVE = 'SHUTDOWN_SAVE'
    SHUTDOWN_NOSAVE = 'SHUTDOWN_NOSAVE'

    def bgrewriteaof(self):
        """Asynchronously rewrite the append-only file."""
        fut = self.execute(b'BGREWRITEAOF')
        return wait_ok(fut)

    def bgsave(self):
        """Asynchronously save the dataset to disk."""
        fut = self.execute(b'BGSAVE')
        return wait_ok(fut)

    def client_kill(self):
        """Kill the connection of a client.

        .. warning:: Not Implemented
        """
        raise NotImplementedError

    def client_list(self):
        """Get the list of client connections.

        Returns list of ClientInfo named tuples.
        """
        fut = self.execute(b'CLIENT', b'LIST', encoding='utf-8')
        return wait_convert(fut, to_tuples)

    def client_getname(self, encoding=_NOTSET):
        """Get the current connection name."""
        return self.execute(b'CLIENT', b'GETNAME', encoding=encoding)

    def client_pause(self, timeout):
        """Stop processing commands from clients for *timeout* milliseconds.

        :raises TypeError: if timeout is not int
        :raises ValueError: if timeout is less than 0
        """
        if not isinstance(timeout, int):
            raise TypeError("timeout argument must be int")
        if timeout < 0:
            raise ValueError("timeout must be greater equal 0")
        fut = self.execute(b'CLIENT', b'PAUSE', timeout)
        return wait_ok(fut)

    def client_reply(self):
        raise NotImplementedError()

    def client_setname(self, name):
        """Set the current connection name."""
        fut = self.execute(b'CLIENT', b'SETNAME', name)
        return wait_ok(fut)

    def command(self):
        """Get array of Redis commands."""
        # TODO: convert result
        return self.execute(b'COMMAND', encoding='utf-8')

    def command_count(self):
        """Get total number of Redis commands."""
        return self.execute(b'COMMAND', b'COUNT')

    def command_getkeys(self, command, *args, encoding='utf-8'):
        """Extract keys given a full Redis command."""
        return self.execute(b'COMMAND', b'GETKEYS', command, *args,
                            encoding=encoding)

    def command_info(self, command, *commands):
        """Get array of specific Redis command details."""
        return self.execute(b'COMMAND', b'INFO', command, *commands,
                            encoding='utf-8')

    def config_get(self, parameter='*'):
        """Get the value of a configuration parameter(s).

        If called without argument will return all parameters.

        :raises TypeError: if parameter is not string
        """
        if not isinstance(parameter, str):
            raise TypeError("parameter must be str")
        fut = self.execute(b'CONFIG', b'GET', parameter, encoding='utf-8')
        return wait_make_dict(fut)

    def config_rewrite(self):
        """Rewrite the configuration file with the in memory configuration."""
        fut = self.execute(b'CONFIG', b'REWRITE')
        return wait_ok(fut)

    def config_set(self, parameter, value):
        """Set a configuration parameter to the given value."""
        if not isinstance(parameter, str):
            raise TypeError("parameter must be str")
        fut = self.execute(b'CONFIG', b'SET', parameter, value)
        return wait_ok(fut)

    def config_resetstat(self):
        """Reset the stats returned by INFO."""
        fut = self.execute(b'CONFIG', b'RESETSTAT')
        return wait_ok(fut)

    def dbsize(self):
        """Return the number of keys in the selected database."""
        return self.execute(b'DBSIZE')

    def debug_sleep(self, timeout):
        """Suspend connection for timeout seconds."""
        fut = self.execute(b'DEBUG', b'SLEEP', timeout)
        return wait_ok(fut)

    def debug_object(self, key):
        """Get debugging information about a key."""
        return self.execute(b'DEBUG', b'OBJECT', key)

    def debug_segfault(self, key):
        """Make the server crash."""
        # won't test, this probably works
        return self.execute(b'DEBUG', 'SEGFAULT')  # pragma: no cover

    def flushall(self, async_op=False):
        """
        Remove all keys from all databases.

        :param async_op: lets the entire dataset to be freed asynchronously. \
        Defaults to False
        """
        if async_op:
            fut = self.execute(b'FLUSHALL', b'ASYNC')
        else:
            fut = self.execute(b'FLUSHALL')
        return wait_ok(fut)

    def flushdb(self, async_op=False):
        """
        Remove all keys from the current database.

        :param async_op: lets a single database to be freed asynchronously. \
        Defaults to False
        """
        if async_op:
            fut = self.execute(b'FLUSHDB', b'ASYNC')
        else:
            fut = self.execute(b'FLUSHDB')
        return wait_ok(fut)

    def info(self, section='default'):
        """Get information and statistics about the server.

        If called without argument will return default set of sections.
        For available sections, see http://redis.io/commands/INFO

        :raises ValueError: if section is invalid

        """
        if not section:
            raise ValueError("invalid section")
        fut = self.execute(b'INFO', section, encoding='utf-8')
        return wait_convert(fut, parse_info)

    def lastsave(self):
        """Get the UNIX time stamp of the last successful save to disk."""
        return self.execute(b'LASTSAVE')

    def monitor(self):
        """Listen for all requests received by the server in real time.

        .. warning::
           Will not be implemented for now.
        """
        # NOTE: will not implement for now;
        raise NotImplementedError

    def role(self):
        """Return the role of the server instance.

        Returns named tuples describing role of the instance.
        For fields information see http://redis.io/commands/role#output-format
        """
        fut = self.execute(b'ROLE', encoding='utf-8')
        return wait_convert(fut, parse_role)

    def save(self):
        """Synchronously save the dataset to disk."""
        return self.execute(b'SAVE')

    def shutdown(self, save=None):
        """Synchronously save the dataset to disk and then
        shut down the server.
        """
        if save is self.SHUTDOWN_SAVE:
            return self.execute(b'SHUTDOWN', b'SAVE')
        elif save is self.SHUTDOWN_NOSAVE:
            return self.execute(b'SHUTDOWN', b'NOSAVE')
        else:
            return self.execute(b'SHUTDOWN')

    def slaveof(self, host, port=None):
        """Make the server a slave of another instance,
        or promote it as master.

        Calling ``slaveof(None)`` will send ``SLAVEOF NO ONE``.

        .. versionchanged:: v0.2.6
           ``slaveof()`` form deprecated
           in favour of explicit ``slaveof(None)``.
        """
        if host is None and port is None:
            return self.execute(b'SLAVEOF', b'NO', b'ONE')
        return self.execute(b'SLAVEOF', host, port)

    def slowlog_get(self, length=None):
        """Returns the Redis slow queries log."""
        if length is not None:
            if not isinstance(length, int):
                raise TypeError("length must be int or None")
            return self.execute(b'SLOWLOG', b'GET', length)
        else:
            return self.execute(b'SLOWLOG', b'GET')

    def slowlog_len(self):
        """Returns length of Redis slow queries log."""
        return self.execute(b'SLOWLOG', b'LEN')

    def slowlog_reset(self):
        """Resets Redis slow queries log."""
        fut = self.execute(b'SLOWLOG', b'RESET')
        return wait_ok(fut)

    def sync(self):
        """Redis-server internal command used for replication."""
        return self.execute(b'SYNC')

    def time(self):
        """Return current server time."""
        fut = self.execute(b'TIME')
        return wait_convert(fut, to_time)


def _split(s):
    k, v = s.split('=')
    return k.replace('-', '_'), v


def to_time(obj):
    return int(obj[0]) + int(obj[1]) * 1e-6


def to_tuples(value):
    line, *lines = value.splitlines(False)
    line = list(map(_split, line.split(' ')))
    ClientInfo = namedtuple('ClientInfo', ' '.join(k for k, v in line))
    # TODO: parse flags and other known fields
    result = [ClientInfo(**dict(line))]
    for line in lines:
        result.append(ClientInfo(**dict(map(_split, line.split(' ')))))
    return result


def parse_info(info):
    res = {}
    for block in info.split('\r\n\r\n'):
        section, *block = block.strip().splitlines()
        section = section[2:].lower()
        res[section] = tmp = {}
        for line in block:
            key, value = line.split(':', 1)
            if ',' in line and '=' in line:
                value = dict(map(lambda i: i.split('='), value.split(',')))
            tmp[key] = value
    return res


# XXX: may change in future
#      (may be hard to maintain for new/old redis versions)
MasterInfo = namedtuple('MasterInfo', 'role replication_offset slaves')
MasterSlaveInfo = namedtuple('MasterSlaveInfo', 'ip port ack_offset')

SlaveInfo = namedtuple('SlaveInfo',
                       'role master_ip master_port state received')

SentinelInfo = namedtuple('SentinelInfo', 'role masters')


def parse_role(role):
    type_ = role[0]
    if type_ == 'master':
        slaves = [MasterSlaveInfo(s[0], int(s[1]), int(s[2]))
                  for s in role[2]]
        return MasterInfo(role[0], int(role[1]), slaves)
    elif type_ == 'slave':
        return SlaveInfo(role[0], role[1], int(role[2]), role[3], int(role[4]))
    elif type_ == 'sentinel':
        return SentinelInfo(*role)
    return role
