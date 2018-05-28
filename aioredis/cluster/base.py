import asyncio

from aioredis.errors import RedisClusterError
from aioredis.util import _NOTSET


class RedisClusterBase:
    """By default Redis commands will simply be sent to the appropriate node of
    the cluster. This class overwrites some commands with special handling"""

    async def keys(self, pattern, *, encoding=_NOTSET):
        res = await self._execute_nodes('keys', pattern, encoding=encoding)
        return [item for part in res for item in part]

    async def scan(self, cursor=0, match=None, count=None):
        """Incrementally iterate the keys space.

        :param match - str
        :param count - int
        :param cursor
        Usage example:

        >>> match = 'key*'
        ... for keys in await cluster.scan(match=match)
        ...     print('Matched:', keys)

        """
        async def scan_coroutine(address, cur=cursor):
            """

            :param address - address tuple
            :param cur
            Usage example:
            """
            if not cur:
                cur = b'0'
            ks = []
            while cur:
                fut = await self._execute_node(
                    address, b'SCAN', cursor=cur, match=match, count=count)
                cur, values = fut
                ks.extend(values)
            return ks

        result = await asyncio.gather(*[
            scan_coroutine(e, cur=cursor) for e in self._get_nodes_entities()
        ], loop=self._loop)

        flatten_result = []
        list(map(flatten_result.extend, result))
        return flatten_result

    async def cluster_del_slots(self, slot, *slots, many=False, slaves=False):
        """
        Set hash slots as unbound in the cluster.
        It determines by itself what node the slot is in and sends it there

        If many = True wil be applied to all master nodes, with slaves = True
        it will affect all nodes
        """

        if many:
            return await self._execute_nodes(
                'cluster_del_slots', slot, *slots, slaves=slaves
            )

        slots = set((slot,) + slots)
        if not all(isinstance(s, int) for s in slots):
            raise TypeError("All parameters must be of type int")

        max_slot = self._cluster_manager.REDIS_CLUSTER_HASH_SLOTS
        if not all(max_slot >= s >= 0 for s in slots):
            raise RedisClusterError('Wrong slot {}.'.format(slot))

        nodes = {}
        for slot in slots:
            node = self._cluster_manager.get_node_by_slot(slot)
            if not node:
                continue

            node_slots = nodes.setdefault(node.address, set())
            node_slots.add(slot)

        if not nodes:
            raise RedisClusterError(
                'Redis cluster don\'t know about these slot(s)'
            )

        if len(nodes) == 1:
            n_address, n_slots = nodes.popitem()
            return await self._execute_node(
                n_address, 'cluster_del_slots', *n_slots
            )

        return await asyncio.gather(*[
            self._execute_node(n_address, 'cluster_del_slots', *n_slots)
            for n_address, n_slots in nodes.items()
        ], loop=self._loop)

    async def cluster_reset(self, *, hard=False, address=None):
        """Reset a Redis Cluster node. Or all nodes if address not provided"""
        if not address:
            return await self._execute_nodes(
                'cluster_reset', slaves=True, hard=hard
            )

        return await self._execute_node(
            address, 'cluster_reset', hard=hard
        )

    async def cluster_add_slots(self, slot, *slots, address=None):
        """
        :param slot:
        :param slots:
        :param address: - address tuple
        :return:
        """
        if not address:
            node = self._cluster_manager.get_random_master_node()
            address = node.address

        return await self._execute_node(
            address, 'cluster_add_slots', slot, *slots
        )

    async def cluster_forget(self, node_id, address=None):
        if address:
            return await self._execute_node(
                address, 'cluster_forget', node_id
            )

        nodes = self._get_nodes_entities()
        for node in self._cluster_manager.slaves:
            if node.id == node_id:
                continue

            nodes.append(node.address)

        return await asyncio.gather(*[
            self._execute_node(add, 'cluster_forget', node_id)
            for add in nodes
        ], loop=self._loop)

    async def cluster_count_key_in_slots(self, slot):
        """Return the number of local keys in the specified hash slot."""
        try:
            slot = int(slot)
        except ValueError:
            raise TypeError(
                "Expected slot to be of type int, got {}".format(type(slot))
            )

        node = self._cluster_manager.get_node_by_slot(slot)
        return await self._execute_node(
            node.address, 'cluster_count_key_in_slots', slot
        )

    async def cluster_get_keys_in_slots(
            self, slot, count, *, encoding=_NOTSET
    ):
        """Return local key names in the specified hash slot."""
        try:
            slot = int(slot)
        except ValueError:
            raise TypeError(
                "Expected slot to be of type int, got {}".format(type(slot))
            )

        node = self._cluster_manager.get_node_by_slot(slot)
        return await self._execute_node(
            node.address, 'cluster_get_keys_in_slots', slot, count,
            encoding=encoding
        )

    async def cluster_failover(self, address, force=False):
        """
        Forces the slave to start a manual failover of its master instance.
        """
        return await self._execute_node(
            address, 'cluster_failover', force=force
        )

    async def cluster_readonly(self, address):
        """
        Enables read queries for a connection to a Redis Cluster slave node.
        """
        return await self._execute_node(address, 'cluster_readonly')

    async def cluster_readwrite(self, address):
        """
        Disables read queries for a connection to a Redis Cluster slave node.
        """
        return await self._execute_node(address, 'cluster_readwrite')
