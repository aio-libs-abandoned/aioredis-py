import math
import os
import shutil
import socket
import stat
import subprocess
import textwrap
import time

from aioredis.log import logger

REDIS_SERVER_EXEC = os.environ.get('REDIS_SERVER_EXEC') or 'redis-server'
REDIS_SLOT_COUNT = 16384
_MAX_RETRY_ERRORS = 4
_ATTEMPT_INTERVAL = 0.3


class TestCluster:
    """This class allows to create a local Redis cluster for test purposes.
    It also includes methods to stop and restart nodes to test failover
    behaviour.

    Parameters:
        - *ports*: The cluster will use the ports from ports list
        - *directory* is used to store the configuration files of all
        processes.
        - *node_timeout*: The cluster node timeout in millliseconds,
        see http://redis.io/topics/cluster-tutorial.

    """
    def __init__(self, ports, directory, node_timeout=3000,
                 server_exec=REDIS_SERVER_EXEC, assign_slots=True):
        self.redis_count = len(ports)
        self.ports = ports
        self.directory = os.path.abspath(directory)
        self.node_timeout = node_timeout
        self.processes = {}
        self._new_directories = set()
        self._exec = server_exec
        self._assign_slots_in_setup = assign_slots

    def setup(self):
        self._setup_directory()
        self._create_redis_directories()
        for port in self.get_ports():
            self._start_redis(port)
        self.configure_cluster()

    def terminate(self):
        for process in self.processes.values():
            process.terminate()
        for process in self.processes.values():
            process.wait(1)

    def clear_directories(self):
        for directory in self._new_directories:
            try:
                self._delete_directory_contents(directory)
                os.rmdir(directory)
            except PermissionError as error:
                print(error)

    def stop_redis(self, port):
        if port not in self.processes:
            raise ValueError('No Redis running at port {}.'.format(port))
        process = self.processes.pop(port)
        process.terminate()
        process.wait(1)

    def restart_redis(self, port):
        if port in self.processes:
            raise ValueError('Redis process at port {} is still running.'
                             .format(port))

        self._start_redis(port)

    def get_ports(self):
        return self.ports

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

            self._write_redis_config_file(os.path.join(
                redis_directory, 'redis.conf'), port)
            self._new_directories.add(redis_directory)

    def _start_redis(self, port):
        directory = self._get_redis_directory(port)
        self.processes[port] = subprocess.Popen(
            [self._exec, 'redis.conf'], cwd=directory)

    def configure_cluster(self):
        time.sleep(_ATTEMPT_INTERVAL)  # Give cluster some time to start up

        addresses = [('127.0.0.1', port) for port in self.get_ports()]
        sockets = self._connect_sockets(addresses)
        masters_count = math.ceil(self.redis_count / 2)
        masters = sockets[:masters_count]
        master_addresses = addresses[:masters_count]
        slaves = sockets[masters_count:]
        master_node_ids = [self._determine_node_id(master, address)
                           for master, address in zip(masters, addresses)]

        if self._assign_slots_in_setup:
            self._assign_slots(masters, master_addresses)
        self._send_meet_messages_to_all(sockets, addresses)
        # MEET messages need some time to propagate
        time.sleep(_ATTEMPT_INTERVAL)
        self._send_replicate_messages(slaves, master_node_ids)

        if self._assign_slots_in_setup:
            # cluster never becomes 'ok' if slots are unbound
            self._wait_until_cluster_state_ok(sockets)

        for sock in sockets:
            sock.close()

    def _connect_sockets(self, addresses):
        return self._retry(socket.create_connection, addresses,
                           error_message='Could not connect to Redis.')

    def _determine_node_id(self, socket, address):
        socket.sendall(b'CLUSTER NODES\r\n')
        data = self._read_bulk_string_response(socket)
        node_id = data[:40].decode('utf-8')
        logger.debug("Master at {} has node id {}.".format(address, node_id))
        return node_id

    def _assign_slots(self, masters, addresses):
        slot_boundaries = [math.floor(i * REDIS_SLOT_COUNT / len(masters))
                           for i in range(len(masters) + 1)]
        slot_ranges = [range(b1, b2)
                       for b1, b2 in zip(slot_boundaries, slot_boundaries[1:])]
        for master, slot_range, address in zip(
                masters, slot_ranges, addresses):
            logger.debug(
                "Assigning master at {} slots {}-{}"
                .format(address, slot_range.start, slot_range.stop - 1)
            )
            slots = ' '.join(str(slot) for slot in slot_range)
            try:
                self._send_command_and_expect_ok(
                    master, 'CLUSTER ADDSLOTS {}\r\n'.format(slots))
            except IOError as e:
                raise IOError(
                    "ADDSLOTS failed. Maybe a cluster is already running? "
                    "({}).".format(str(e))
                )

    def _send_meet_messages_to_all(self, sockets, addresses):
        for i, sock in enumerate(sockets):
            for j, address in enumerate(addresses):
                if i != j:
                    self._send_command_and_expect_ok(
                        sock, 'CLUSTER MEET {} {}\r\n'.format(*address))

    def _send_replicate_messages(self, slaves, master_node_ids):
        def _send_replicate_message(arg):
            slave, master_node_id = arg
            self._send_command_and_expect_ok(
                slave, 'CLUSTER REPLICATE {}\r\n'.format(master_node_id))

        self._retry(
            _send_replicate_message,
            list(zip(slaves, master_node_ids)),
            'Replication failed.'
        )

    def _wait_until_cluster_state_ok(self, sockets):
        def _check_state(socket):
            socket.sendall(b'CLUSTER INFO\r\n')
            data = self._read_bulk_string_response(socket).decode('utf-8')
            if 'cluster_state:ok' not in data:
                raise IOError('Cluster state not ok')

        self._retry(
            _check_state,
            sockets,
            error_message='Cluster state not ok.',
            max_errors=10
        )

    def _retry(self, method, arguments, error_message,
               max_errors=_MAX_RETRY_ERRORS, interval=_ATTEMPT_INTERVAL):
        results = [None] * len(arguments)
        successful_indexes = []
        errors = 0
        while len(successful_indexes) < len(arguments):
            for i, argument in enumerate(arguments):
                if i not in successful_indexes:
                    try:
                        results[i] = method(argument)
                        successful_indexes.append(i)
                    except (IOError, ConnectionRefusedError):
                        errors += 1
                        if errors >= max_errors:
                            raise IOError(
                                error_message +
                                ' Stop retrying after {} errors.'
                                .format(errors)
                            )
                        else:
                            logger.info(
                                error_message + ' Will retry after {}s.'
                                .format(interval)
                            )
                            time.sleep(interval)

        return results

    def _recv_until(self, socket, delimiter):
        data = b''
        while delimiter not in data:
            data += socket.recv(1024)

        index = data.index(delimiter)
        return data[:index], data[index + len(delimiter):]

    def _recv_bytes(self, socket, byte_count):
        data = b''
        while len(data) < byte_count:
            received = socket.recv(min(1024, byte_count - len(data)))
            if len(received) == 0:
                raise IOError('Socket closed')
            else:
                data += received
        return data

    def _send_command_and_expect_ok(self, socket, command):
        socket.sendall(command.encode('utf-8'))
        response, _ = self._recv_until(socket, b'\r\n')
        if response != b'+OK':
            raise IOError(response.decode('utf-8'))

    def _read_bulk_string_response(self, socket):
        header, data = self._recv_until(socket, b'\r\n')
        if header[0] != ord('$'):
            raise ValueError('Expected bulk string response.')
        byte_count = int(header[1:].decode('utf-8'))
        missing_byte_count = byte_count - len(data) + 2
        if missing_byte_count > 0:
            remaining_data = self._recv_bytes(socket, missing_byte_count)
            if remaining_data[-2:] != b'\r\n':
                raise ValueError('Invalid bulk string received.')
            data += remaining_data[:-2]
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
