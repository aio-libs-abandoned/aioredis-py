import asyncio

from ..util import wait_ok
from ..commands import Redis
from .pool import create_sentinel_pool
from .parser import (
    parse_sentinel_masters,
    parse_sentinel_get_master,
    parse_sentinel_master,
    parse_sentinel_slaves_and_sentinels,
    )


@asyncio.coroutine
def create_sentinel(sentinels, *, loop=None):
    """Creates Redis Sentinel client.

    `sentinels` is a list of sentinel nodes.
    """

    if loop is None:
        loop = asyncio.get_event_loop()

    pool = yield from create_sentinel_pool(sentinels, loop=loop)
    return RedisSentinel(pool)


class RedisSentinel:
    """Redis Sentinel client."""

    def __init__(self, pool):
        # What I need in here -- special Pool controlling Sentinels
        self._pool = pool
        #
        # Add dict of pools:
        #   master -> pool;
        #
        # Need a way to know when any connection gets closed
        # to be able to reconnect and rediscover redis nodes.
        #
        # 'connection closed' message must be propagated to
        # Sentinel manager and nodes discovery must be started again;
        #
        # two ways: either use pool subclass or add signals.

    def close(self):
        """Close client connections."""
        self._pool.close()

    @asyncio.coroutine
    def wait_closed(self):
        """Coroutine waiting until underlying connections are closed."""
        yield from self._pool.wait_closed()

    @property
    def closed(self):
        """True if connection is closed."""
        return self._pool.closed

    def get_master(self, name):
        """Returns Redis client to master Redis server."""
        return Redis(self._pool.get_master(name))

    def get_slave(self, name):
        """Returns Redis client to slave Redis server."""
        return Redis(self._pool.get_slave(name))

    def execute(self, command, *args, **kwargs):
        return self._pool.execute(
            b'SENTINEL', command, *args, **kwargs)

    def ping(self):
        """Send PING command to Sentinel instance(s)."""  # instances?
        return (yield from self._pool.execute(b'PING'))

    def master(self, name):
        """Returns a dictionary containing the specified masters state."""
        fut = self.execute(b'MASTER', name, encoding='utf-8')
        return parse_sentinel_master(fut)

    def master_address(self, name):
        """Returns a (host, port) pair for the given ``name``."""
        fut = self.execute(b'get-master-addr-by-name', name, encoding='utf-8')
        return parse_sentinel_get_master(fut)

    def masters(self):
        """Returns a list of dictionaries containing each master's state."""
        masters = self.execute(b'MASTERS', encoding='utf-8')
        # TODO: process masters
        return parse_sentinel_masters(masters)

    def slaves(self, name):
        """Returns a list of slaves for ``name``"""
        fut = self.execute(b'SLAVES', name, encoding='utf-8')
        return parse_sentinel_slaves_and_sentinels(fut)

    def sentinels(self, name):
        """Returns a list of sentinels for ``name``"""
        fut = self.execute(b'SENTINELS', name, encoding='utf-8')
        return parse_sentinel_slaves_and_sentinels(fut)

    def monitor(self, name, ip, port, quorum):
        """Add a new master to Sentinel to be monitored"""
        fut = self.execute(b'MONITOR', name, ip, port, quorum)
        return wait_ok(fut)

    def remove(self, name):
        """Remove a master from Sentinel's monitoring"""
        fut = self.execute(b'REMOVE', name)
        return wait_ok(fut)

    def set(self, name, option, value):
        """Set Sentinel monitoring parameters for a given master"""
        return self.execute(b"SET", name, option, value)

    def failover(self, name):
        """Force a failover of a named master."""

    def check_quorum(self, name):
        """
        Check if the current Sentinel configuration is able
        to reach the quorum needed to failover a master,
        and the majority needed to authorize the failover.
        """
        return self.execute(b'CKQUORUM', name)
