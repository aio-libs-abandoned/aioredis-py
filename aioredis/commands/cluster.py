from aioredis.util import wait_ok


class ClusterCommandsMixin:
    """Cluster commands mixin.

    For commands details see: http://redis.io/commands#cluster
    """

    def cluster_add_slots(self, slot, *slots):
        """Assign new hash slots to receiving node."""
        slots = (slot,) + slots
        if not all(isinstance(s, int) for s in slots):
            raise TypeError("All parameters must be of type int")
        fut = self._conn.execute(b'CLUSTER', b'ADDSLOTS', *slots)
        return wait_ok(fut)

    def cluster_count_failure_reports(self, node_id):
        """Return the number of failure reports active for a given node."""
        return self._conn.execute(
            b'CLUSTER', b'COUNT-FAILURE-REPORTS', node_id)

    def cluster_count_key_in_slots(self, slot):
        """Return the number of local keys in the specified hash slot."""
        if not isinstance(slot, int):
            raise TypeError("Expected slot to be of type int, got {}"
                            .format(type(slot)))
        return self._conn.execute(b'CLUSTER', b'COUNTKEYSINSLOT', slot)

    def cluster_del_slots(self, slot, *slots):
        """Set hash slots as unbound in receiving node."""
        slots = (slot,) + slots
        if not all(isinstance(s, int) for s in slots):
            raise TypeError("All parameters must be of type int")
        fut = self._conn.execute(b'CLUSTER', b'DELSLOTS', *slots)
        return wait_ok(fut)

    def cluster_failover(self):
        """Forces a slave to perform a manual failover of its master."""
        pass    # TODO: Implement

    def cluster_forget(self, node_id):
        """Remove a node from the nodes table."""
        fut = self._conn.execute(b'CLUSTER', b'FORGET', node_id)
        return wait_ok(fut)

    def cluster_get_keys_in_slots(self, slot, count, *, encoding):
        """Return local key names in the specified hash slot."""
        return self._conn.execute(b'CLUSTER', b'GETKEYSINSLOT', slot, count,
                                  encoding=encoding)

    def cluster_info(self):
        """Provides info about Redis Cluster node state."""
        pass    # TODO: Implement

    def cluster_keyslot(self, key):
        """Returns the hash slot of the specified key."""
        return self._conn.execute(b'CLUSTER', b'KEYSLOT', key)

    def cluster_meet(self, ip, port):
        """Force a node cluster to handshake with another node."""
        fut = self._conn.execute(b'CLUSTER', b'MEET', ip, port)
        return wait_ok(fut)

    def cluster_nodes(self):
        """Get Cluster config for the node."""
        pass    # TODO: Implement

    def cluster_replicate(self, node_id):
        """Reconfigure a node as a slave of the specified master node."""
        fut = self._conn.execute(b'CLUSTER', b'REPLICATE', node_id)
        return wait_ok(fut)

    def cluster_reset(self, *, hard=False):
        """Reset a Redis Cluster node."""
        reset = hard and b'HARD' or b'SOFT'
        fut = self._conn.execute(b'CLUSTER', b'RESET', reset)
        return wait_ok(fut)

    def cluster_save_config(self):
        """Force the node to save cluster state on disk."""
        fut = self._conn.execute(b'CLUSTER', b'SAVECONFIG')
        return wait_ok(fut)

    def cluster_set_config_epoch(self, config_epoch):
        """Set the configuration epoch in a new node."""
        fut = self._conn.execute(b'CLUSTER', b'SET-CONFIG-EPOCH', config_epoch)
        return wait_ok(fut)

    def cluster_setslot(self, slot, command, node_id):
        """Bind a hash slot to specified node."""
        pass    # TODO: Implement

    def cluster_slaves(self, node_id):
        """List slave nodes of the specified master node."""
        pass    # TODO: Implement

    def cluster_slots(self):
        """Get array of Cluster slot to node mappings."""
        pass    # TODO: Implement
