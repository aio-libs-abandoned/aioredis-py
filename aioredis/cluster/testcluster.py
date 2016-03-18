import os, os.path
import shutil
import stat
import subprocess
import textwrap
import time
import urllib.request
import urllib.error

REDIS_SERVER_EXEC = 'redis-server'
REDIS_TRIB_URL = 'https://raw.githubusercontent.com/antirez/redis/unstable/src/redis-trib.rb'
REDIS_TRIB = os.path.basename(REDIS_TRIB_URL)


def setup_test_cluster(*args, **kwargs):
    cluster = TestCluster(*args, **kwargs)
    cluster.setup()
    return cluster


__all__ = ['setup_test_cluster']


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
        self._download_redis_trib_file()
        for port in self.get_ports():
            self._start_redis(port)
        self._configure_cluster()

    def terminate(self):
        for process in self.processes.values():
            process.terminate()

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

    def _download_redis_trib_file(self):
        target = os.path.join(self.directory, REDIS_TRIB)
        if not os.path.exists(target):
            try:
                response = urllib.request.urlopen(REDIS_TRIB_URL)
                html = response.read()
            except urllib.error.URLError:
                raise urllib.error.URLError(
                    'Could not download the redis-trib.rb file necessary to configure the cluster. '
                    'File is located at {}'.format(REDIS_TRIB_URL))

            with open(target, 'wb') as file:
                file.write(html)

            self._make_executable(target)

    def _start_redis(self, port):
        directory = self._get_redis_directory(port)
        self.processes[port] = subprocess.Popen([REDIS_SERVER_EXEC, 'redis.conf'], cwd=directory)

    def _configure_cluster(self):
        # TODO: stop using the redis-trib script (which needs to be downloaded and needs a 'yes' answer)
        # and setup cluster directly.
        executable = os.path.join(self.directory, REDIS_TRIB)
        redis_args = ['127.0.0.1:{}'.format(port) for port in self.get_ports()]
        process = subprocess.Popen(
            [executable, 'create', '--replicas', '1'] + redis_args,
            cwd=self.directory,
            stdin=subprocess.PIPE
        )
        time.sleep(1)
        try:
            process.communicate(b'yes')
        except BrokenPipeError:
            raise BrokenPipeError(
                'Could not communicate with redis-trib.rb script. '
                'Maybe there is already a cluster running in the specified directory.'
            )

        # If we terminate a master now, no failover election is started.
        # Apparently, nodes need some time after startup before election is possible.
        time.sleep(0.2)

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

    @staticmethod
    def _make_executable(path):
        os.chmod(path, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
