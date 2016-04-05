import argparse
import os
import sys

from aioredis.cluster import testcluster

assert sys.version >= '3.3', 'Please use Python 3.3 or higher.'

START_PORT = 7001
REDIS_COUNT = 6


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Set up a Redis cluster for the unittests.")
    parser.add_argument(
        '--dir',
        default='redis-cluster',
        help='Directory for the Redis cluster. '
             'Must be empty or nonexistent, unless -f is specified.'
    )

    return parser.parse_args()


def setup_test_cluster(args):
    directory = os.path.abspath(os.path.expanduser(args.dir))
    testcluster.setup_test_cluster(REDIS_COUNT, START_PORT, directory)


if __name__ == '__main__':
    args = parse_arguments()
    setup_test_cluster(args)
    print(
        "Cluster has been set up. Use 'python runclustertests.py' "
        " to run the tests. "
        "To stop the cluster, simply kill the processes."
    )
