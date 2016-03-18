import math
import os
import shutil
import socket
import stat
import subprocess
import textwrap
import time

REDIS_SERVER_EXEC = 'redis-server'
REDIS_SLOT_COUNT = 16384
_SOCKET_READ_BYTE_COUNT = 1024  # must be large enough to hold a CLUSTER INFO response
_MAX_ATTEMPTS = 4
_ATTEMPT_INTERVAL = 0.3


from aioredis.log import logger
__all__ = ['setup_test_cluster']


def setup_test_cluster(*args, **kwargs):
    cluster = TestCluster(*args, **kwargs)
    cluster.setup()
    return cluster


class TestCluster:
    """This class allows to create a local Redis cluster for test purposes. It also includes methods to stop and
    restart nodes to test failover behaviour.
    """
    def __init__(self, redis_count, start_port, directory, node_timeout=3000):
        self.redis_count = redis_count
        self.start_port = start_port
        self.directory = os.path.abspath(directory)
        self.node_timeout = node_timeout
        self.processes = {}

    def setup(self):
        self._setup_directory()
        self._create_redis_directories()
        for port in self.get_ports():
            self._start_redis(port)
        self._configure_cluster()

    def terminate(self):
        for process in self.processes.values():
            process.terminate()
        for process in self.processes.values():
            process.wait(1)

    def stop_redis(self, port):
        if port not in self.processes:
            raise ValueError('No Redis running at port {}.'.format(port))
        process = self.processes.pop(port)
        process.terminate()
        process.wait(1)

    def restart_redis(self, port):
        if port in self.processes:
            raise ValueError('Redis process at port {} is still running.'.format(port))

        self._start_redis(port)

    def get_ports(self):
        return [self.start_port + i for i in range(self.redis_count)]

    def _setup_directory(self):
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

    def _create_redis_directories(self):
        for port in self.get_ports():
            redis_directory = self._get_redis_directory(port)
            if not os.path.exists(redis_directory):
                os.mkdir(redis_directory)
            else:
                self._delete_directory_contents(redis_directory)

            self._write_redis_config_file(os.path.join(redis_directory, 'redis.conf'), port)

    def _start_redis(self, port):
        directory = self._get_redis_directory(port)
        self.processes[port] = subprocess.Popen([REDIS_SERVER_EXEC, 'redis.conf'], cwd=directory)

    def _configure_cluster(self):
        time.sleep(_ATTEMPT_INTERVAL)  # Give cluster some time to start up

        addresses = [('127.0.0.1', port) for port in self.get_ports()]
        sockets = self._connect_sockets(addresses)
        masters_count = math.ceil(self.redis_count / 2)
        masters = sockets[:masters_count]
        master_addresses = addresses[:masters_count]
        slaves = sockets[masters_count:]
        master_node_ids = [self._determine_node_id(master, address) for master, address in zip(masters, addresses)]

        self._assign_slots(masters, master_addresses)
        self._send_meet_messages_to_all(sockets, addresses)
        time.sleep(_ATTEMPT_INTERVAL)  # MEET messages need some time to propagate
        self._wait_and_send_replicate_messages(slaves, master_node_ids)
        self._wait_until_cluster_state_ok(sockets)

        for socket in sockets:
            socket.close()

    def _connect_sockets(self, addresses):
        sockets = {}
        attempt = 0
        while len(sockets) < len(addresses):
            attempt += 1
            for address in addresses:
                if address in sockets:
                    continue

                try:
                    sockets[address] = socket.create_connection(address)
                except ConnectionRefusedError as e:
                    if attempt >= _MAX_ATTEMPTS:
                        raise IOError(
                            'Could not connect to cluster after {} attempts.'.format(_MAX_ATTEMPTS)
                        )
                    logger.info('Connection failed, will retry. ({})'.format(str(e)))
                    time.sleep(_ATTEMPT_INTERVAL)

        return [sockets[address] for address in addresses]

    def _determine_node_id(self, socket, address):
        socket.sendall(b'CLUSTER NODES\r\n')
        data = self._read_bulk_string_response(socket)
        node_id = data[:40].decode('utf-8')
        logger.debug("Master at {} has node id {}.".format(address, node_id))
        return node_id

    def _assign_slots(self, masters, addresses):
        slot_boundaries = [math.floor(i * REDIS_SLOT_COUNT / len(masters)) for i in range(len(masters) + 1)]
        slot_ranges = [range(b1, b2) for b1, b2 in zip(slot_boundaries, slot_boundaries[1:])]
        for master, slot_range, address in zip(masters, slot_ranges, addresses):
            logger.debug("Assigning master at {} slots {}-{}".format(address, slot_range.start, slot_range.stop - 1))
            slots = ' '.join(str(slot) for slot in slot_range)
            try:
                self._send_command_and_expect_ok(master, 'CLUSTER ADDSLOTS {}\r\n'.format(slots))
            except IOError as e:
                raise IOError('ADDSLOTS failed. Maybe a cluster is already running? ({}).'.format(str(e)))

    def _send_meet_messages_to_all(self, sockets, addresses):
        for i, socket in enumerate(sockets):
            for j, address in enumerate(addresses):
                if i != j:
                    self._send_command_and_expect_ok(socket, 'CLUSTER MEET {} {}\r\n'.format(*address))

    def _wait_and_send_replicate_messages(self, slaves, master_node_ids):
        replicated = []
        attempt = 0
        while len(replicated) < len(slaves):
            attempt += 1
            for slave, master_node_id in zip(slaves, master_node_ids):
                if slave in replicated:
                    continue

                try:
                    self._send_command_and_expect_ok(slave, 'CLUSTER REPLICATE {}\r\n'.format(master_node_id))
                    replicated.append(slave)
                except IOError as e:
                    if attempt >= _MAX_ATTEMPTS:
                        raise IOError(
                            'Replication still not successful after {} attempts.'.format(_MAX_ATTEMPTS)
                        )
                    logger.info('Replication failed, will retry. ({})'.format(str(e)))
                    time.sleep(_ATTEMPT_INTERVAL)

    def _wait_until_cluster_state_ok(self, sockets):
        state_ok = []
        attempt = 0
        while len(state_ok) < len(sockets):
            attempt += 1
            for socket in sockets:
                if socket in state_ok:
                    continue

                socket.sendall(b'CLUSTER INFO\r\n')
                data = self._read_bulk_string_response(socket).decode('utf-8')
                if 'cluster_state:ok' in data:
                    state_ok.append(socket)
                else:
                    if attempt >= _MAX_ATTEMPTS:
                        raise IOError('Cluster state is still not ok after {} attempts.'.format(_MAX_ATTEMPTS))
                    time.sleep(_ATTEMPT_INTERVAL)

    def _send_command_and_expect_ok(self, socket, command):
        socket.sendall(command.encode('utf-8'))
        response = socket.recv(_SOCKET_READ_BYTE_COUNT).decode('utf-8')
        if not response.startswith('+OK\r\n'):
            raise IOError("Redis command failed. Response was: " + response)

    def _read_bulk_string_response(self, socket):
        data = socket.recv(10)
        if data[0] != ord('$'):
            raise ValueError('Expected bulk string response.')
        byte_count, data = data[1:].split(b'\r\n')
        byte_count = int(byte_count.decode('utf-8'))
        data += socket.recv(byte_count - len(data))
        end = socket.recv(2)
        if end != b'\r\n':
            raise ValueError('Invalid bulk string received.')
        return data

    def _get_redis_directory(self, port):
        return os.path.join(self.directory, 'redis-{}'.format(port))

    @staticmethod
    def _delete_directory_contents(directory):
        for name in os.listdir(directory):
            path = os.path.join(directory, name)
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)

    def _write_redis_config_file(self, path, port):
        with open(path, 'w') as file:
            file.write(textwrap.dedent("""
                port {}
                cluster-enabled yes
                cluster-config-file nodes.conf
                cluster-node-timeout {}
                """.format(port, self.node_timeout)
            ))

        # Protect against the CONFIG REWRITE test
        self._remove_write_access(path)

    @staticmethod
    def _remove_write_access(path):
        os.chmod(path, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
