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
    def __init__(self, redis_count, start_port, directory, node_timeout=5000):
        self.redis_count = redis_count
        self.start_port = start_port
        self.directory = os.path.abspath(directory)
        self.node_timeout = node_timeout
        self.processes = []

    def setup(self):
        self._setup_directory()
        redis_directories = self._create_redis_directories()
        self._download_redis_trib_file()
        self._spawn_redises(redis_directories)
        self._configure_cluster()

    def terminate(self):
        for process in self.processes:
            process.terminate()

    def get_ports(self):
        return [self.start_port + i for i in range(self.redis_count)]

    def _setup_directory(self):
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

    def _create_redis_directories(self):
        redis_directories = []
        for port in self.get_ports():
            redis_directory = os.path.join(self.directory, 'redis-{}'.format(port))
            redis_directories.append(redis_directory)
            if not os.path.exists(redis_directory):
                os.mkdir(redis_directory)
            else:
                self._delete_directory_contents(redis_directory)

            self._write_redis_config_file(os.path.join(redis_directory, 'redis.conf'), port)

        return redis_directories

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

    def _spawn_redises(self, directories):
        for directory in directories:
            process = subprocess.Popen([REDIS_SERVER_EXEC, 'redis.conf'], cwd=directory)
            self.processes.append(process)

    def _configure_cluster(self):
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
