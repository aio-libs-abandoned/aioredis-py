from aioredis.util import (
    wait_ok, wait_convert, decode, _NOTSET
)


class ClusterCommandsMixin:
    """Cluster commands mixin.

    For commands details see: http://redis.io/commands#cluster
    """

    def cluster_add_slots(self, slot, *slots):
        """Assign new hash slots to receiving node."""
        slots = set((slot, ) + slots)
        if not all(isinstance(s, int) for s in slots):
            raise TypeError("All parameters must be of type int")

        fut = self.execute(b'CLUSTER', b'ADDSLOTS', *slots)
        return wait_ok(fut)

    def cluster_count_failure_reports(self, node_id):
        """Return the number of failure reports active for a given node."""
        return self.execute(
            b'CLUSTER', b'COUNT-FAILURE-REPORTS', node_id)

    def cluster_count_key_in_slots(self, slot):
        """Return the number of local keys in the specified hash slot."""

        return self.execute(b'CLUSTER', b'COUNTKEYSINSLOT', slot)

    def cluster_del_slots(self, slot, *slots):
        """Set hash slots as unbound in receiving node."""
        slots = set((slot,) + slots)
        if not all(isinstance(s, int) for s in slots):
            raise TypeError("All parameters must be of type int")

        fut = self.execute(b'CLUSTER', b'DELSLOTS', *slots)
        return wait_ok(fut)

    def cluster_failover(self, force=False):
        """
        Forces the slave to start a manual failover of its master instance.
        """
        command = force and b'FORCE' or b'TAKEOVER'

        fut = self.execute(b'CLUSTER', b'FAILOVER', command)
        return wait_ok(fut)

    def cluster_forget(self, node_id):
        """Remove a node from the nodes table."""
        fut = self.execute(b'CLUSTER', b'FORGET', node_id)
        return wait_ok(fut)

    def cluster_get_keys_in_slots(self, slot, count, *, encoding=_NOTSET):
        """Return local key names in the specified hash slot."""
        return self.execute(b'CLUSTER', b'GETKEYSINSLOT', slot, count,
                            encoding=encoding)

    def cluster_info(self):
        """Provides info about Redis Cluster node state."""
        fut = self.execute(b'CLUSTER', b'INFO')
        return wait_convert(fut, parse_info, encoding=self.encoding)

    def cluster_keyslot(self, key):
        """Returns the hash slot of the specified key."""
        return self.execute(b'CLUSTER', b'KEYSLOT', key)

    def cluster_meet(self, ip, port):
        """Force a node cluster to handshake with another node."""
        fut = self.execute(b'CLUSTER', b'MEET', ip, port)
        return wait_ok(fut)

    def cluster_nodes(self):
        """Get Cluster config for the node."""
        fut = self.execute(b'CLUSTER', b'NODES')
        return wait_convert(fut, parse_cluster_nodes, encoding=self.encoding)

    def cluster_replicate(self, node_id):
        """Reconfigure a node as a slave of the specified master node."""
        fut = self.execute(b'CLUSTER', b'REPLICATE', node_id)
        return wait_ok(fut)

    def cluster_reset(self, *, hard=False):
        """Reset a Redis Cluster node."""
        reset = hard and b'HARD' or b'SOFT'
        fut = self.execute(b'CLUSTER', b'RESET', reset)
        return wait_ok(fut)

    def cluster_save_config(self):
        """Force the node to save cluster state on disk."""
        fut = self.execute(b'CLUSTER', b'SAVECONFIG')
        return wait_ok(fut)

    def cluster_set_config_epoch(self, config_epoch):
        """Set the configuration epoch in a new node."""

        try:
            config_epoch = int(config_epoch)
        except ValueError:
            raise TypeError(
                "Expected slot to be of type int, got {}".format(
                    type(config_epoch)
                )
            )

        fut = self.execute(b'CLUSTER', b'SET-CONFIG-EPOCH', config_epoch)
        return wait_ok(fut)

    def cluster_setslot(self, slot, command, node_id=None):
        """Bind a hash slot to specified node."""
        raise NotImplementedError()

    def cluster_slaves(self, node_id):
        """List slave nodes of the specified master node."""
        fut = self.execute(b'CLUSTER', b'SLAVES', node_id)
        return wait_convert(
            fut, parse_cluster_nodes_lines, encoding=self.encoding
        )

    def cluster_slots(self):
        """Get array of Cluster slot to node mappings."""
        fut = self.execute(b'CLUSTER', b'SLOTS')
        return wait_convert(fut, parse_cluster_slots)

    def cluster_readonly(self):
        """
        Enables read queries for a connection to a Redis Cluster slave node.
        """
        fut = self.execute(b'READONLY')
        return wait_ok(fut)

    def cluster_readwrite(self):
        """
        Disables read queries for a connection to a Redis Cluster slave node.
        """
        fut = self.execute(b'READWRITE')
        return wait_ok(fut)


def _decode(s, encoding):
    if encoding:
        return decode(s, encoding)
    return s


def parse_info(resp, **options):
    e = options.get('encoding')

    lines = resp.strip().splitlines()
    result = list(map(lambda s: s.split(_decode(b':', e)), lines))
    return dict(result)


def parse_node_slots(raw_slots, encoding=None):
    """
    @see: https://redis.io/commands/cluster-nodes#serialization-format
    @see: https://redis.io/commands/cluster-nodes#special-slot-entries
    """

    slots, migrations = [], []
    migration_delimiter = _decode(b'->-', encoding)
    import_delimiter = _decode(b'-<-', encoding)
    range_delimiter = _decode(b'-', encoding)
    migrating_state = _decode(b'migrating', encoding)
    importing_state = _decode(b'importing', encoding)

    for r in raw_slots.strip().split():
        if migration_delimiter in r:
            slot_id, dst_node_id = r[1:-1].split(migration_delimiter, 1)
            migrations.append({
                'slot': int(slot_id),
                'node_id': dst_node_id,
                'state': migrating_state
            })
        elif import_delimiter in r:
            slot_id, src_node_id = r[1:-1].split(import_delimiter, 1)
            migrations.append({
                'slot': int(slot_id),
                'node_id': src_node_id,
                'state': importing_state
            })
        elif range_delimiter in r:
            start, end = r.split(range_delimiter)
            slots.append((int(start), int(end)))
        else:
            slots.append((int(r), int(r)))

    return tuple(slots), tuple(migrations)


def parse_cluster_nodes_lines(lines, encoding=None):
    """
    @see: https://redis.io/commands/cluster-nodes # list of string
    """

    address_splitter = _decode(b':', encoding)
    port_splitter = _decode(b'@', encoding)
    flags_splitter = _decode(b',', encoding)
    no_master = _decode(b'-', encoding)

    for line in lines:
        parts = line.split(None, 8)
        self_id, addr, flags, master_id, ping_sent, \
            pong_recv, config_epoch, link_state = parts[:8]

        host, port = addr.rsplit(address_splitter, 1)
        nat_port = None

        if port_splitter in port:
            # Since version 4.0.0 address_node_info has the format
            # '192.1.2.3:7001@17001
            at_index = port.index(port_splitter)
            nat_port = int(port[at_index + 1:])
            port = port[:at_index]

        node = {
            'id': self_id,
            'host': host,
            'port': int(port),
            'nat-port': nat_port,
            'flags': tuple(flags.split(flags_splitter)),
            'master': master_id if master_id != no_master else None,
            'ping-sent': int(ping_sent),
            'pong-recv': int(pong_recv),
            'config_epoch': int(config_epoch),
            'status': link_state,
            'slots': tuple(),
            'migrations': tuple(),
        }

        if len(parts) >= 9:
            slots, migrations = parse_node_slots(parts[8], encoding)
            node['slots'], node['migrations'] = slots, migrations

        yield node


def parse_cluster_nodes(resp, encoding=None):
    """
    @see: http://redis.io/commands/cluster-nodes  # string
    """

    yield from parse_cluster_nodes_lines(resp.strip().splitlines(), encoding)


def parse_cluster_slots(resp):
    """
    @see https://redis.io/commands/cluster-slots
    """

    slots = {}
    for slot in resp:
        start, end, master = slot[:3]
        slots[(start, end)] = tuple(master[:2])

    return slots
