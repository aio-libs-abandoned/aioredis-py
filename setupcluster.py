import argparse
import os
import sys

from aioredis.cluster.testcluster import TestCluster

assert sys.version >= '3.5', 'Please use Python 3.5 or higher.'

START_PORT = 7001
REDIS_COUNT = 6


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Set up a Redis cluster to run the examples.")
    parser.add_argument(
        '--dir',
        default='redis-cluster',
        help='Directory for the Redis cluster. '
             'Must be empty or nonexistent, unless -f is specified.'
    )

    return parser.parse_args()


def setup_test_cluster(args):
    directory = os.path.abspath(os.path.expanduser(args.dir))
    cluster = TestCluster(list(range(START_PORT, START_PORT + REDIS_COUNT)), directory)
    cluster.setup()


if __name__ == '__main__':
    args = parse_arguments()
    setup_test_cluster(args)
    print(
        "Cluster has been set up."
        "To stop the cluster, simply kill the processes."
    )
