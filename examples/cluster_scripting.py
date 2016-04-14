import logging

from aioredis.errors import RedisClusterError

logging.basicConfig(level=logging.INFO)

NODES = [('localhost', port) for port in range(7001, 7007)]
KEYS = ['{evalkey}:1', '{evalkey}:2']


def main():

    loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def connect():
        try:
            return (yield from create_cluster(
                NODES, loop=loop, encoding='utf8'))
        except RedisClusterError:
            raise RedisClusterError(
                "Could not connect to cluster. Did you start it with "
                "the setupcluster.py script?"
            )

    @asyncio.coroutine
    def clear_keys(cluster):
        for key in KEYS:
            yield from cluster.delete(key)

    @asyncio.coroutine
    def use_eval(cluster):
        script = """
        if redis.call('setnx', KEYS[1], ARGV[1]) == 1
        then
            return ARGV[2]
        else
            redis.call('set', KEYS[2], ARGV[1])
            return ARGV[3]
        end
        """

        res = yield from cluster.eval(
            script, keys=KEYS,
            args=['data'] + ['Stored in {}'.format(key) for key in KEYS])
        print(res)
        for key in KEYS:
            value = yield from cluster.get(key)
            print("{} -> {}".format(key, value))

        yield from cluster.clear()  # closing all open connections

    try:
        cluster = loop.run_until_complete(connect())
        for coroutine in (clear_keys, use_eval, use_eval):
            loop.run_until_complete(coroutine(cluster))
    finally:
        loop.close()


if __name__ == '__main__':
    import sys
    import os.path
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.append(root)
    import asyncio
    from aioredis import create_cluster
    main()
