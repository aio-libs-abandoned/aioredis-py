

class ClusterCommandsMixin:
    """Cluster commands mixin.

    For commands details see: http://redis.io/commands#cluster
    """

    def cluster_add_slots(self, slot, *slots):
        """Assign new hash slots to receiving node."""
        pass

    def cluster_count_failure_reports(self, node_id):
        """Return the number of failure reports active for a given node."""
        pass

    def cluster_count_key_in_slots(self, slot):
        """Return the number of local keys in the specified hash slot."""
        pass

    def cluster_del_slots(self, slot, *slots):
        """Set hash slots as unbound in receiving node."""
        pass

    def cluster_failover(self):
        """Forces a slave to perform a manual failover of its master."""
        pass

    def cluster_forget(self, node_id):
        """Remove a node from the nodes table."""
        pass

    def cluster_get_keys_in_slots(self, slot, count):
        """Return local key names in the specified hash slot."""
        pass

    def cluster_info(self):
        """Provides info about Redis Cluster node state."""
        pass

    def cluster_keyslot(self, key):
        """Returns the hash slot of the specified key."""
        pass

    def cluster_meet(self, ip, port):
        """Force a node cluster to handshake with another node."""
        pass

    def cluster_nodes(self):
        """Get Cluster config for the node."""
        pass

    def cluster_replicate(self, node_id):
        """Reconfigure a node as a slave of the specified master node."""
        pass

    def cluster_reset(self, *, hard=False):
        """Reset a Redis Cluster node."""
        pass

    def cluster_save_config(self):
        """Force the node to save cluster state on disk."""
        pass

    def cluster_set_config_epoch(self, config_epoch):
        """Set the configuration epoch in a new node."""
        pass

    def cluster_setslot(self, slot, command, node_id):
        """Bind a hash slot to specified node."""
        pass

    def cluster_slaves(self, node_id):
        """List slave nodes of the specified master node."""
        pass

    def cluster_slots(self):
        """Get array of Cluster slot to node mappings."""
        pass
