import argparse
import os, os.path
import shutil
import stat
import subprocess
import sys
import textwrap
import time
import urllib.request

assert sys.version >= '3.3', 'Please use Python 3.3 or higher.'

START_PORT = 7001
REDIS_COUNT = 6
REDIS_SERVER_EXEC = 'redis-server'
REDIS_PROCESSES = []
REDIS_TRIB_URL = 'https://raw.githubusercontent.com/antirez/redis/unstable/src/redis-trib.rb'
REDIS_TRIB = os.path.basename(REDIS_TRIB_URL)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Set up a Redis cluster and run all unittests")
    parser.add_argument(
        '--dir',
        default='redis-cluster',
        help='Directory for the Redis cluster. Must be empty or nonexistent, unless -f is specified.'
    )

    return parser.parse_args()


def setup_redis_cluster(args):
    base_directory, directories = create_redis_directories(args)
    download_redis_trib_file(base_directory)
    spawn_redises(directories)
    configure_cluster(base_directory)


def create_redis_directories(args):
    base_directory = setup_base_directory(args)
    redis_directories = []
    for i, port in enumerate(get_ports()):
        redis_directory = os.path.join(base_directory, 'redis{}'.format(i+1))
        redis_directories.append(redis_directory)
        if not os.path.exists(redis_directory):
            os.mkdir(redis_directory)
        else:
            delete_directory_contents(redis_directory)
        write_redis_config_file(os.path.join(redis_directory, 'redis.conf'), port)

    return base_directory, redis_directories


def download_redis_trib_file(base_directory):
    target = os.path.join(base_directory, REDIS_TRIB)
    if not os.path.exists(target):
        response = urllib.request.urlopen(REDIS_TRIB_URL)
        html = response.read()
        with open(target, 'wb') as file:
            file.write(html)

        make_executable(target)


def spawn_redises(directories):
    for directory in directories:
        process = subprocess.Popen([REDIS_SERVER_EXEC, 'redis.conf'], cwd=directory)
        REDIS_PROCESSES.append(process)


def configure_cluster(base_directory):
    executable = os.path.join(base_directory, REDIS_TRIB)
    redis_args = ['127.0.0.1:{}'.format(port) for port in get_ports()]
    process = subprocess.Popen(
        [executable, 'create', '--replicas', '1'] + redis_args,
        cwd=base_directory,
        stdin=subprocess.PIPE
    )
    time.sleep(1)
    process.communicate(b'yes')


def get_ports():
    return [START_PORT + i for i in range(REDIS_COUNT)]


def setup_base_directory(args):
    base_directory = os.path.abspath(os.path.expanduser(args.dir))
    if not os.path.exists(base_directory):
        os.makedirs(base_directory)

    return base_directory


def delete_directory_contents(directory):
    for name in os.listdir(directory):
        path = os.path.join(directory, name)
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)


def write_redis_config_file(path, port):
    with open(path, 'w') as file:
        file.write(textwrap.dedent("""
            port {}
            cluster-enabled yes
            cluster-config-file nodes.conf
            cluster-node-timeout 5000
            """.format(port)
        ))

    # Protect against the CONFIG REWRITE test
    remove_write_access(path)


def remove_write_access(path):
    os.chmod(path, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)


def make_executable(path):
    os.chmod(path, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


if __name__ == '__main__':
    args = parse_arguments()
    setup_redis_cluster(args)
