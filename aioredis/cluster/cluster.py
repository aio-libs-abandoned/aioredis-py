import asyncio
import random
from functools import partial

from aioredis.commands import (
    create_redis,
    Redis,
    create_redis_pool
)
from aioredis.errors import ProtocolError
from aioredis.errors import ReplyError, RedisClusterError
from aioredis.log import logger
from aioredis.util import decode, encode_str, cached_property
from .base import RedisClusterBase
from .crc import crc16

__all__ = (
    'create_pool_cluster',
    'RedisPoolCluster',
    'create_cluster',
    'RedisCluster',
)


def parse_moved_response_error(err):
    if not err or not err.args or not err.args[0]:
        return
    data = err.args[0].strip()
    if not data.startswith('MOVED'):
        return
    try:
        host, port = data.split()[-1].split(':')
        return host, int(port)
    except IndexError:
        return


class ClusterNode:
    def __init__(
            self, number, id, host, port, flags, master, status, slots,
            **kwargs
    ):
        self.id = id
        self.host = host
        self.port = port
        self.flags = flags
        self.master = master
        self.status = status
        self.slots = slots
        self.number = number

    def __repr__(self):
        return r'Address: {!r}. Master: {!r}. Slave: {!r}. Alive: {!r}'.format(
            self.address, self.is_master, self.is_slave, self.is_alive)

    @cached_property
    def is_master(self):
        return 'master' in self.flags

    @cached_property
    def is_slave(self):
        return 'slave' in self.flags

    @cached_property
    def address(self):
        return self.host, self.port

    @cached_property
    def is_alive(self):
        return ('fail' not in self.flags and
                'fail?' not in self.flags and
                self.status == 'connected')

    def in_range(self, value):
        if not self.slots:
            return False

        if value < self.slots[0][0]:
            return False
        if value > self.slots[-1][-1]:
            return False
        return any(rng[0] <= value <= rng[1] for rng in self.slots)


class ClusterNodesManager:

    REDIS_CLUSTER_HASH_SLOTS = 16384

    def __init__(self, nodes):
        nodes = list(nodes)
        masters_slots = {node.id: node.slots for node in nodes}
        for node in nodes:
            if node.is_slave:
                node.slots = masters_slots[node.master]
        self.nodes = nodes

    def __repr__(self):
        return r' == '.join(repr(node) for node in self.nodes)

    def __str__(self):
        return '\n'.join(repr(node) for node in self.nodes)

    @classmethod
    def parse_info(cls, info):
        for index, node_data in enumerate(info):
            yield ClusterNode(index, **node_data)

    @classmethod
    def create(cls, data):
        nodes = cls.parse_info(data)
        return cls(nodes)

    @staticmethod
    def key_slot(key, bucket=REDIS_CLUSTER_HASH_SLOTS):
        """Calculate key slot for a given key.

        :param key - str|bytes
        :param bucket - int
        """
        k = encode_str(key)
        start = k.find(b'{')
        if start > -1:
            end = k.find(b'}', start + 1)
            if end > -1 and end != start + 1:
                k = k[start + 1:end]
        return crc16(k) % bucket

    @cached_property
    def alive_nodes(self):
        return [node for node in self.nodes if node.is_alive]

    @cached_property
    def nodes_count(self):
        return len(self.alive_nodes)

    @cached_property
    def masters_count(self):
        return len(self.masters)

    @cached_property
    def slaves_count(self):
        return len(self.slaves)

    @cached_property
    def masters(self):
        return [node for node in self.alive_nodes if node.is_master]

    @cached_property
    def slaves(self):
        return [node for node in self.alive_nodes if node.is_slave]

    @cached_property
    def all_slots_covered(self):
        covered_slots_number = sum(
            end - start + 1
            for master in self.masters for start, end in master.slots
        )
        return covered_slots_number >= self.REDIS_CLUSTER_HASH_SLOTS

    def get_node_by_slot(self, slot):
        for node in self.masters:
            if node.in_range(slot):
                return node
        else:
            return None

    def get_node_by_id(self, node_id):
        for node in self.nodes:
            if node_id == node.id:
                return node
        else:
            return None

    def get_node_by_address(self, address):
        for node in self.nodes:
            if address == node.address:
                return node
        else:
            return None

    def get_random_node(self):
        return random.choice(self.alive_nodes)

    def get_random_master_node(self):
        return random.choice(self.masters)

    def get_random_slave_node(self):
        return random.choice(self.slaves)

    def determine_slot(self, *keys):
        if any(key is None for key in keys):
            raise TypeError('key must not be None')
        if len(keys) == 1:
            return self.key_slot(keys[0])
        else:
            slots = {self.key_slot(key) for key in keys}
            if len(slots) != 1:
                raise RedisClusterError(
                    'all keys must map to the same key slot')
            return slots.pop()


async def create_pool_cluster(
        nodes, *, db=0, password=None, encoding=None,
        minsize=10, maxsize=10, commands_factory=Redis, loop=None):
    """
    Create Redis Pool Cluster.

    :param nodes = [(address1, port1), (address2, port2), ...]
    :param db - int
    :param password: str
    :param encoding: str
    :param minsize: int
    :param maxsize: int
    :param commands_factory: obj
    :param loop: obj
    :return RedisPoolCluster instance.
    """
    if not nodes or not isinstance(nodes, (tuple, list)):
        raise RedisClusterError(
            'Cluster nodes is not set properly. {0}'.
            format(create_pool_cluster.__doc__))

    cluster = RedisPoolCluster(
        nodes, db, password, encoding=encoding, minsize=minsize,
        maxsize=maxsize, commands_factory=commands_factory, loop=loop)
    await cluster.initialize()
    return cluster


async def create_cluster(
        nodes, *, db=0, password=None, encoding=None,
        commands_factory=Redis, loop=None):
    """
    Create Redis Pool Cluster.

    :param nodes = [(address1, port1), (address2, port2), ...]
    :param db - int
    :param password: str
    :param encoding: str
    :param commands_factory: obj
    :param loop: obj
    :return RedisPoolCluster instance.
    """
    if not nodes or not isinstance(nodes, (tuple, list)):
        raise RedisClusterError(
            'Cluster nodes is not set properly. {0}'.
            format(create_cluster.__doc__))

    cluster = RedisCluster(
        nodes, db, password, encoding=encoding,
        commands_factory=commands_factory, loop=loop)
    await cluster.initialize()
    return cluster


class RedisCluster(RedisClusterBase):
    """Redis cluster."""

    MAX_MOVED_COUNT = 10

    def __init__(self, nodes, db=0, password=None, encoding=None,
                 *, commands_factory, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        self._nodes = nodes
        self._db = db
        self._password = password
        self._encoding = encoding
        self._factory = commands_factory
        self._loop = loop
        self._moved_count = 0
        self._cluster_manager = None

    def _is_eval_command(self, command):
        if isinstance(command, bytes):
            command = command.decode('utf-8')
        return command.lower() in ['eval', 'evalsha']

    def get_node(self, command, *args, **kwargs):
        if self._is_eval_command(command):
            keys = kwargs.get('keys', [])
            if not isinstance(keys, (list, tuple)):
                raise TypeError('keys must be given as list or tuple')
        else:
            keys = args[:1]

        if len(keys) > 0:
            slot = self._cluster_manager.determine_slot(*keys)
            node = self._cluster_manager.get_node_by_slot(slot)
            if node is not None:
                return node

        return self._cluster_manager.get_random_master_node()

    def node_count(self):
        return self._cluster_manager.nodes_count

    def masters_count(self):
        return self._cluster_manager.masters_count

    def slave_count(self):
        return self._cluster_manager.slaves_count

    def _get_nodes_entities(self, slaves=False):
        slave_nodes = []
        if slaves:
            slave_nodes = [node.address for node in self.slave_nodes]
        return [node.address for node in self.master_nodes] + slave_nodes

    @property
    def master_nodes(self):
        return self._cluster_manager.masters

    @property
    def slave_nodes(self):
        return self._cluster_manager.slaves

    async def _get_raw_cluster_info_from_node(self, node):
        conn = await create_redis(
            node,
            db=self._db,
            password=self._password,
            encoding='utf-8',
            commands_factory=self._factory,
            loop=self._loop
        )

        try:
            nodes_resp = await conn.cluster_nodes()
            return nodes_resp
        finally:
            conn.close()
            await conn.wait_closed()

    async def fetch_cluster_info(self):
        logger.info('Loading cluster info from {}...'.format(self._nodes))
        tasks = [
            asyncio.ensure_future(
                self._get_raw_cluster_info_from_node(node), loop=self._loop
            ) for node in self._nodes
        ]
        try:
            for task in asyncio.as_completed(tasks, loop=self._loop):
                try:
                    nodes_raw_response = list(await task)
                    self._cluster_manager = ClusterNodesManager.create(
                        nodes_raw_response
                    )
                    logger.info('Cluster info loaded successfully: %s',
                                nodes_raw_response)
                    return
                except (ReplyError, ProtocolError,
                        ConnectionError, OSError) as exc:
                    logger.warning(
                        "Loading cluster info from a node failed with {}"
                        .format(repr(exc))
                    )
        finally:
            for task in tasks:
                task.cancel()
            # Wait until all tasks have closed their connection
            await asyncio.gather(
                *tasks, loop=self._loop, return_exceptions=True)

        raise RedisClusterError(
            "No cluster info could be loaded from any host")

    async def initialize(self):
        logger.info('Initializing cluster...')
        self._moved_count = 0
        await self.fetch_cluster_info()
        logger.info('Initialized cluster.\n{}'.format(self._cluster_manager))

    async def clear(self):
        pass  # All connections are created on demand and destroyed afterwards.

    @property
    def all_slots_covered(self):
        return self._cluster_manager.all_slots_covered

    async def create_connection(self, address):
        conn = await create_redis(
            address,
            db=self._db,
            encoding=self._encoding,
            password=self._password,
            commands_factory=self._factory,
            loop=self._loop,
        )
        return conn

    async def _execute_node(self, address, command, *args, **kwargs):
        """Execute redis command and returns Future waiting for the answer.

        :param command str
        :param pool obj
        Raises:
        * TypeError if any of args can not be encoded as bytes.
        * ReplyError on redis '-ERR' responses.
        * ProtocolError when response can not be decoded meaning connection
          is broken.
        """
        cmd = decode(command, 'utf-8').lower()
        to_close = []
        try:
            conn = await self.create_connection(address)
            to_close.append(conn)
            return await getattr(conn, cmd)(*args, **kwargs)
        except ReplyError as err:
            address = parse_moved_response_error(err)
            if address is None:
                raise
            logger.debug('Got MOVED command: {}'.format(err))
            self._moved_count += 1
            if self._moved_count >= self.MAX_MOVED_COUNT:
                await self.initialize()
                node = self.get_node(command, *args, **kwargs)
                address = node.address
            conn = await self.create_connection(address)
            to_close.append(conn)
            return await getattr(conn, cmd)(*args, **kwargs)
        finally:
            for conn in to_close:
                conn.close()
                await conn.wait_closed()

    async def _execute_nodes(self, command, *args, slaves=False, **kwargs):
        """
        Execute redis command for all nodes and returns
        Future waiting for the answer.

        :param command str
        :param slaves bool - Execute on all nodes masters + slaves
        Raises:
        * TypeError if any of args can not be encoded as bytes.
        * ReplyError on redis '-ERR' responses.
        * ProtocolError when response can not be decoded meaning connection
          is broken.
        """
        nodes = self._get_nodes_entities(slaves=slaves)
        return await asyncio.gather(*[
            self._execute_node(node, command, *args, **kwargs)
            for node in nodes
        ], loop=self._loop)

    async def execute(
            self, command, *args, address=None, many=False, slaves=False,
            **kwargs
    ):
        """Execute redis command and returns Future waiting for the answer.

        :param command str
        :param address tuple - Execute on node with specified address
            if many specified will be ignored
        :param many bool - invoke on all master nodes
        :param slaves bool - if many specified, execute even on slave nodes
        Raises:
        * TypeError if any of args can not be encoded as bytes.
        * ReplyError on redis '-ERR' responses.
        * ProtocolError when response can not be decoded meaning connection
          is broken.
        """

        if many:
            return await self._execute_nodes(
                command, *args, slaves=slaves, **kwargs
            )

        if not address:
            address = self.get_node(command, *args, **kwargs).address

        return await self._execute_node(address, command, *args, **kwargs)

    def __getattr__(self, cmd):
        return partial(self.execute, cmd)


class RedisPoolCluster(RedisCluster):
    """
    Redis pool cluster.
    Do not use it for cluster management.
    Will not operate with slaves and target node
    """

    def __init__(self, nodes, db=0, password=None, encoding=None,
                 *, minsize, maxsize, commands_factory, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        super().__init__(nodes, db=db, password=password, encoding=encoding,
                         commands_factory=commands_factory, loop=loop)
        self._minsize = minsize
        self._maxsize = maxsize
        self._cluster_pool = {}

    def _get_nodes_entities(self, **kwargs):
        return self._cluster_pool.values()

    async def get_cluster_pool(self):
        cluster_pool = {}
        nodes = list(self._cluster_manager.masters)
        tasks = [
            create_redis_pool(
                node.address,
                db=self._db,
                password=self._password,
                encoding=self._encoding,
                minsize=self._minsize,
                maxsize=self._maxsize,
                commands_factory=self._factory,
                loop=self._loop
            )
            for node in nodes
        ]
        results = await asyncio.gather(*tasks, loop=self._loop)

        for node, connection in zip(nodes, results):
            cluster_pool[node.id] = connection
        return cluster_pool

    async def reload_cluster_pool(self):
        logger.info('Reloading cluster...')
        await self.clear()
        self._moved_count = 0
        await self.fetch_cluster_info()
        logger.info('Connecting to cluster...')
        self._cluster_pool = await self.get_cluster_pool()
        logger.info('Reloaded cluster')

    async def initialize(self):
        await super().initialize()
        self._cluster_pool = await self.get_cluster_pool()

    async def clear(self):
        """Clear pool connections. Close and remove all free connections."""
        for pool in self._get_nodes_entities():
            pool.close()
            await pool.wait_closed()

    def get_node(self, command, *args, **kwargs):
        node = super().get_node(command, *args, **kwargs)
        return self._cluster_pool[node.id]

    async def _execute_node(self, pool, command, *args, **kwargs):
        """Execute redis command and returns Future waiting for the answer.

        :param command str
        :param pool obj
        Raises:
        * TypeError if any of args can not be encoded as bytes.
        * ReplyError on redis '-ERR' responses.
        * ProtocolError when response can not be decoded meaning connection
          is broken.
        """
        cmd = decode(command, 'utf-8').lower()
        try:
            with await pool as conn:
                return await getattr(conn, cmd)(*args, **kwargs)
        except ReplyError as err:
            address = parse_moved_response_error(err)
            if address is None:
                raise

            logger.debug('Got MOVED command: {}'.format(err))
            self._moved_count += 1
            if self._moved_count >= self.MAX_MOVED_COUNT:
                await self.initialize()
                pool = self.get_node(command, *args, **kwargs)
                with await pool as conn:
                    return await getattr(conn, cmd)(*args, **kwargs)
            else:
                conn = await self.create_connection(address)
                res = await getattr(conn, cmd)(*args, **kwargs)
                conn.close()
                await conn.wait_closed()
                return res

    async def execute(self, command, *args, many=False, **kwargs):
        """Execute redis command and returns Future waiting for the answer.

        :param command str
        :param many bool - invoke on all master nodes
        Raises:
        * TypeError if any of args can not be encoded as bytes.
        * ReplyError on redis '-ERR' responses.
        * ProtocolError when response can not be decoded meaning connection
          is broken.
        """

        if many:
            return await self._execute_nodes(command, *args, **kwargs)

        pool = self.get_node(command, *args, **kwargs)
        return await self._execute_node(pool, command, *args, **kwargs)
