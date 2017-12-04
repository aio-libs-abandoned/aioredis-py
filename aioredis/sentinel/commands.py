import asyncio

from ..util import wait_ok, wait_convert
from ..commands import Redis
from .pool import create_sentinel_pool


async def create_sentinel(sentinels, *, db=None, password=None,
                          encoding=None, minsize=1, maxsize=10,
                          ssl=None, timeout=0.2, loop=None):
    """Creates Redis Sentinel client.

    `sentinels` is a list of sentinel nodes.
    """

    if loop is None:
        loop = asyncio.get_event_loop()

    pool = await create_sentinel_pool(sentinels,
                                      db=db,
                                      password=password,
                                      encoding=encoding,
                                      minsize=minsize,
                                      maxsize=maxsize,
                                      ssl=ssl,
                                      timeout=timeout,
                                      loop=loop)
    return RedisSentinel(pool)


class RedisSentinel:
    """Redis Sentinel client."""

    def __init__(self, pool):
        self._pool = pool

    def close(self):
        """Close client connections."""
        self._pool.close()

    async def wait_closed(self):
        """Coroutine waiting until underlying connections are closed."""
        await self._pool.wait_closed()

    @property
    def closed(self):
        """True if connection is closed."""
        return self._pool.closed

    def master_for(self, name):
        """Returns Redis client to master Redis server."""
        # TODO: make class configurable
        return Redis(self._pool.master_for(name))

    def slave_for(self, name):
        """Returns Redis client to slave Redis server."""
        # TODO: make class configurable
        return Redis(self._pool.slave_for(name))

    def execute(self, command, *args, **kwargs):
        """Execute Sentinel command.

        It will be prefixed with SENTINEL automatically.
        """
        return self._pool.execute(
            b'SENTINEL', command, *args, **kwargs)

    async def ping(self):
        """Send PING command to Sentinel instance(s)."""
        # TODO: add kwargs allowing to pick sentinel to send command to.
        return await self._pool.execute(b'PING')

    def master(self, name):
        """Returns a dictionary containing the specified masters state."""
        fut = self.execute(b'MASTER', name, encoding='utf-8')
        return wait_convert(fut, parse_sentinel_master)

    def master_address(self, name):
        """Returns a (host, port) pair for the given ``name``."""
        fut = self.execute(b'get-master-addr-by-name', name, encoding='utf-8')
        return wait_convert(fut, parse_address)

    def masters(self):
        """Returns a list of dictionaries containing each master's state."""
        fut = self.execute(b'MASTERS', encoding='utf-8')
        # TODO: process masters: we can adjust internal state
        return wait_convert(fut, parse_sentinel_masters)

    def slaves(self, name):
        """Returns a list of slaves for ``name``."""
        fut = self.execute(b'SLAVES', name, encoding='utf-8')
        return wait_convert(fut, parse_sentinel_slaves_and_sentinels)

    def sentinels(self, name):
        """Returns a list of sentinels for ``name``."""
        fut = self.execute(b'SENTINELS', name, encoding='utf-8')
        return wait_convert(fut, parse_sentinel_slaves_and_sentinels)

    def monitor(self, name, ip, port, quorum):
        """Add a new master to Sentinel to be monitored."""
        fut = self.execute(b'MONITOR', name, ip, port, quorum)
        return wait_ok(fut)

    def remove(self, name):
        """Remove a master from Sentinel's monitoring."""
        fut = self.execute(b'REMOVE', name)
        return wait_ok(fut)

    def set(self, name, option, value):
        """Set Sentinel monitoring parameters for a given master."""
        fut = self.execute(b"SET", name, option, value)
        return wait_ok(fut)

    def failover(self, name):
        """Force a failover of a named master."""
        fut = self.execute(b'FAILOVER', name)
        return wait_ok(fut)

    def check_quorum(self, name):
        """
        Check if the current Sentinel configuration is able
        to reach the quorum needed to failover a master,
        and the majority needed to authorize the failover.
        """
        return self.execute(b'CKQUORUM', name)


SENTINEL_STATE_TYPES = {
    'can-failover-its-master': int,
    'config-epoch': int,
    'down-after-milliseconds': int,
    'failover-timeout': int,
    'info-refresh': int,
    'last-hello-message': int,
    'last-ok-ping-reply': int,
    'last-ping-reply': int,
    'last-ping-sent': int,
    'master-link-down-time': int,
    'master-port': int,
    'num-other-sentinels': int,
    'num-slaves': int,
    'o-down-time': int,
    'pending-commands': int,
    'link-pending-commands': int,
    'link-refcount': int,
    'parallel-syncs': int,
    'port': int,
    'quorum': int,
    'role-reported-time': int,
    's-down-time': int,
    'slave-priority': int,
    'slave-repl-offset': int,
    'voted-leader-epoch': int,
    'flags': lambda s: frozenset(s.split(',')),  # TODO: make flags enum?
}


def pairs_to_dict_typed(response, type_info):
    it = iter(response)
    result = {}
    for key, value in zip(it, it):
        if key in type_info:
            try:
                value = type_info[key](value)
            except (TypeError, ValueError):
                # if for some reason the value can't be coerced, just use
                # the string value
                pass
        result[key] = value
    return result


def parse_sentinel_masters(response):
    result = {}
    for item in response:
        state = pairs_to_dict_typed(item, SENTINEL_STATE_TYPES)
        result[state['name']] = state
    return result


def parse_sentinel_slaves_and_sentinels(response):
    return [pairs_to_dict_typed(item, SENTINEL_STATE_TYPES)
            for item in response]


def parse_sentinel_master(response):
    return pairs_to_dict_typed(response, SENTINEL_STATE_TYPES)


def parse_address(value):
    if value is not None:
        return (value[0], int(value[1]))
