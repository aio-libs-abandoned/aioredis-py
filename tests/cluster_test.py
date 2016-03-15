import unittest
import unittest.mock
import textwrap
import asyncio

from aioredis import ReplyError, ProtocolError
from aioredis.cluster import RedisCluster, RedisPoolCluster
from aioredis.cluster.cluster import parse_moved_response_error, parse_nodes_info, ClusterNodesManager, ClusterNode
from ._testutil import (
    SLOT_ZERO_KEY, run_until_complete, BaseTest, IS_REDIS_CLUSTER,
    CreateConnectionMock, FakeConnection, PoolConnectionMock
)


# slightly modified example from the cluster spec
RAW_NODE_INFO_DATA = textwrap.dedent("""\
    07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004 slave,fail e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected
    67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002 master,fail? - 0 1426238316232 2 connected 5461-10922
    292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003 master - 0 1426238318243 3 connected 10923-16383
    6ec23923021cf3ffec47632106199cb7f496ce01 127.0.0.1:30005 slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 connected
    824fe116063bc5fcf9f4ffd895bc17aee7731ac3 127.0.0.1:30006 slave 292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 0 1426238317741 6 connected
    e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001 myself,master - 0 0 1 connected 0-5460"""
                                     )


class ParseTest(unittest.TestCase):
    def test_parse_moved_response_error(self):
        self.assertIsNone(parse_moved_response_error(ReplyError()))
        self.assertIsNone(parse_moved_response_error(ReplyError('ASK')))
        self.assertEqual(
            parse_moved_response_error(ReplyError('MOVED 3999 127.0.0.1:6381')),
            ('127.0.0.1', 6381)
        )

    def test_parse_nodes_info(self):
        self.assertTupleEqual(
            list(parse_nodes_info(RAW_NODE_INFO_DATA, ClusterNodesManager.CLUSTER_NODES_TUPLE))[0],
            [
                ('07c37dfeb235213a872192d90877d0cd55635b91', '127.0.0.1', 30004, ('slave', 'fail'), 'e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca', 'connected', ((0, 0), )),
                ('67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1', '127.0.0.1', 30002, ('master', 'fail?'), '0', 'connected', ((5461, 10922), )),
                ('292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f', '127.0.0.1', 30003, ('master', ), '0', 'connected', ((10923, 16383), )),
                ('6ec23923021cf3ffec47632106199cb7f496ce01', '127.0.0.1', 30005, ('slave', ), '67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1', 'connected', ((0, 0), )),
                ('824fe116063bc5fcf9f4ffd895bc17aee7731ac3', '127.0.0.1', 30006, ('slave', ), '292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f', 'connected', ((0, 0), )),
                ('e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca', '127.0.0.1', 30001, ('myself', 'master'), '0', 'connected', ((0, 5460), )),
            ][0]

        )


class ClusterNodesManagerTest(unittest.TestCase):
    def test_key_slot(self):
        self.assertEqual(ClusterNodesManager.key_slot(SLOT_ZERO_KEY), 0)
        self.assertEqual(ClusterNodesManager.key_slot('key'), 12539)
        self.assertEqual(ClusterNodesManager.key_slot(b'key'), 12539)

    def test_create(self):
        manager = ClusterNodesManager.create(RAW_NODE_INFO_DATA)
        self.assertEqual(len(manager.nodes), 6)
        self.assertTrue(all(isinstance(node, ClusterNode) for node in manager.nodes))

    def test_node_count(self):
        manager = ClusterNodesManager.create(RAW_NODE_INFO_DATA)
        self.assertEqual(manager.nodes_count, 4)
        self.assertEqual(manager.masters_count, 2)
        self.assertEqual(manager.slaves_count, 2)

    def test_alive_nodes(self):
        manager = ClusterNodesManager.create(RAW_NODE_INFO_DATA)
        self.assertEqual(manager.alive_nodes, manager.nodes[2:])

    def test_cluster_node(self):
        manager = ClusterNodesManager.create(RAW_NODE_INFO_DATA)
        node1 = manager.nodes[0]
        self.assertFalse(node1.is_master)
        self.assertTrue(node1.is_slave)
        self.assertEqual(node1.address, ('127.0.0.1', 30004))
        self.assertFalse(node1.is_alive)

        node2 = manager.nodes[2]
        self.assertTrue(node2.is_master)
        self.assertFalse(node2.is_slave)
        self.assertTrue(node2.is_alive)

    def test_in_range(self):
        manager = ClusterNodesManager.create(RAW_NODE_INFO_DATA)
        master = manager.nodes[5]
        self.assertTrue(master.in_range(0))
        self.assertTrue(master.in_range(5460))
        self.assertFalse(master.in_range(5461))


@unittest.skipUnless(IS_REDIS_CLUSTER, 'need a running cluster')
class RedisClusterTest(BaseTest):
    @run_until_complete
    def test_create(self):
        cluster = yield from self.create_test_cluster()
        self.assertIsInstance(cluster, RedisCluster)

    @run_until_complete
    def test_counts(self):
        cluster = yield from self.create_test_cluster()
        self.assertEqual(cluster.node_count(), 6)
        self.assertEqual(cluster.masters_count(), 3)
        self.assertEqual(cluster.slave_count(), 3)

    @run_until_complete
    def test_get_node(self):
        cluster = yield from self.create_test_cluster()
        # Compare the redis_trib.rb script used to setup the test cluster
        node = cluster.get_node('key:0')
        self.assertEqual(node.address[1], self.redis_port)
        node = cluster.get_node(b'key:1')
        self.assertEqual(node.address[1], self.redis_port + 1)
        node = cluster.get_node(b'key:3', 'more', 'args')
        self.assertEqual(node.address[1], self.redis_port + 2)

    @run_until_complete
    def test_execute(self):
        cluster = yield from self.create_test_cluster()
        expected_connection = FakeConnection(self, self.redis_port)
        with CreateConnectionMock(self, {self.redis_port: expected_connection}):
            ok = yield from cluster.execute('SET', SLOT_ZERO_KEY, 'value')

        self.assertTrue(ok)
        expected_connection.execute.assert_called_once_with(b'SET', SLOT_ZERO_KEY, 'value')

    @run_until_complete
    def test_execute_with_moved(self):
        cluster = yield from self.create_test_cluster()
        expected_connections = {
            self.redis_port: FakeConnection(
                self, self.redis_port,
                return_value=ReplyError('MOVED 6000 127.0.0.1:{}'.format(self.redis_port + 1))
            ),
            self.redis_port + 1: FakeConnection(self, self.redis_port + 1)
        }
        with CreateConnectionMock(self, expected_connections):
            ok = yield from cluster.execute('SET', SLOT_ZERO_KEY, 'value')

        self.assertTrue(ok)
        expected_connections[self.redis_port].execute.assert_called_once_with(b'SET', SLOT_ZERO_KEY, 'value')
        expected_connections[self.redis_port + 1].execute.assert_called_once_with(b'SET', SLOT_ZERO_KEY, 'value')

    @run_until_complete
    def test_execute_with_reply_error(self):
        cluster = yield from self.create_test_cluster()
        expected_connection = FakeConnection(self, self.redis_port, return_value=ReplyError('ERROR'))
        with CreateConnectionMock(self, {self.redis_port: expected_connection}):
            with self.assertRaises(ReplyError):
                yield from cluster.execute('SET', SLOT_ZERO_KEY, 'value')

        expected_connection.execute.assert_called_once_with(b'SET', SLOT_ZERO_KEY, 'value')

    @run_until_complete
    def test_execute_with_protocol_error(self):
        cluster = yield from self.create_test_cluster()
        expected_connection = FakeConnection(self, self.redis_port, return_value=ProtocolError('ERROR'))
        with CreateConnectionMock(self, {self.redis_port: expected_connection}):
            with self.assertRaises(ProtocolError):
                yield from cluster.execute('SET', SLOT_ZERO_KEY, 'value')

        expected_connection.execute.assert_called_once_with(b'SET', SLOT_ZERO_KEY, 'value')

    @run_until_complete
    def test_execute_many(self):
        cluster = yield from self.create_test_cluster()
        expected_connections = {
            port: FakeConnection(self, port) for port in range(self.redis_port, self.redis_port + 3)
        }

        with CreateConnectionMock(self, expected_connections):
            ok = yield from cluster.execute('PING')

        self.assertEqual(ok, [b'OK'] * 3)
        for connection in expected_connections.values():
            connection.execute.assert_called_once_with('PING', encoding=unittest.mock.ANY)


@unittest.skipUnless(IS_REDIS_CLUSTER, 'need a running cluster')
class RedisPoolClusterTest(BaseTest):
    @asyncio.coroutine
    def create_test_pool_cluster(self):
        nodes = self.get_cluster_addresses(self.redis_port)
        return self.create_pool_cluster(nodes, loop=self.loop)

    @run_until_complete
    def test_create(self):
        cluster = yield from self.create_test_pool_cluster()
        self.assertIsInstance(cluster, RedisPoolCluster)

    @run_until_complete
    def test_get_pool(self):
        cluster = yield from self.create_test_pool_cluster()
        # Compare the redis_trib.rb script used to setup the test cluster
        pool = cluster.get_pool('key:0')
        self.assertEqual(pool._address[1], self.redis_port)
        pool = cluster.get_pool(b'key:1')
        self.assertEqual(pool._address[1], self.redis_port + 1)
        pool = cluster.get_pool(b'key:3', 'more', 'args')
        self.assertEqual(pool._address[1], self.redis_port + 2)

    @run_until_complete
    def test_execute(self):
        cluster = yield from self.create_test_pool_cluster()
        expected_connection = FakeConnection(self, self.redis_port)
        with PoolConnectionMock(self, cluster, {self.redis_port: expected_connection}):
            ok = yield from cluster.execute('SET', SLOT_ZERO_KEY, 'value')

        self.assertTrue(ok)
        expected_connection.execute.assert_called_once_with(b'SET', SLOT_ZERO_KEY, 'value')

    @run_until_complete
    def test_execute_with_moved(self):
        cluster = yield from self.create_test_pool_cluster()
        expected_pool_connection = FakeConnection(
            self, self.redis_port,
            return_value=ReplyError('MOVED 6000 127.0.0.1:{}'.format(self.redis_port + 1))
        )
        expected_direct_connection = FakeConnection(self, self.redis_port + 1)

        with PoolConnectionMock(self, cluster, {self.redis_port: expected_pool_connection}):
            with CreateConnectionMock(self, {self.redis_port + 1: expected_direct_connection}):
                ok = yield from cluster.execute('SET', SLOT_ZERO_KEY, 'value')

        self.assertTrue(ok)
        expected_pool_connection.execute.assert_called_once_with(b'SET', SLOT_ZERO_KEY, 'value')
        expected_direct_connection.execute.assert_called_once_with(b'SET', SLOT_ZERO_KEY, 'value')

    @run_until_complete
    def test_execute_with_reply_error(self):
        cluster = yield from self.create_test_pool_cluster()
        expected_connection = FakeConnection(self, self.redis_port, return_value=ReplyError('ERROR'))
        with PoolConnectionMock(self, cluster, {self.redis_port: expected_connection}):
            with self.assertRaises(ReplyError):
                yield from cluster.execute('SET', SLOT_ZERO_KEY, 'value')

        expected_connection.execute.assert_called_once_with(b'SET', SLOT_ZERO_KEY, 'value')

    @run_until_complete
    def test_execute_with_protocol_error(self):
        cluster = yield from self.create_test_pool_cluster()
        expected_connection = FakeConnection(self, self.redis_port, return_value=ProtocolError('ERROR'))
        with PoolConnectionMock(self, cluster, {self.redis_port: expected_connection}):
            with self.assertRaises(ProtocolError):
                yield from cluster.execute('SET', SLOT_ZERO_KEY, 'value')

        expected_connection.execute.assert_called_once_with(b'SET', SLOT_ZERO_KEY, 'value')

    @run_until_complete
    def test_execute_many(self):
        cluster = yield from self.create_test_pool_cluster()
        expected_connections = {
            port: FakeConnection(self, port) for port in range(self.redis_port, self.redis_port + 3)
        }

        with PoolConnectionMock(self, cluster, expected_connections):
            ok = yield from cluster.execute('PING')

        self.assertEqual(ok, [b'OK'] * 3)
        for connection in expected_connections.values():
            connection.execute.assert_called_once_with('PING', encoding=unittest.mock.ANY)
