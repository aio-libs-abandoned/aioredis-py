import logging

from aioredis.errors import RedisClusterError

logging.basicConfig(level=logging.INFO)

NODES = [('localhost', port) for port in range(7001, 7007)]


def main():

    loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def connect():
        try:
            return (yield from create_pool_cluster(
                NODES, loop=loop, encoding='utf8'))
        except RedisClusterError:
            raise RedisClusterError(
                "Could not connect to cluster. "
                "Did you start it with the setupcluster.py script?"
            )

    @asyncio.coroutine
    def get_key(cluster):
        key = 'key1'
        value = 'value1'
        yield from cluster.set(key, value)
        res = yield from cluster.get(key)
        assert res == value
        print("get_key {} -> {}".format(key, res))
        yield from cluster.clear()  # closing all open connections

    @asyncio.coroutine
    def get_keys(cluster):
        keys = ['key1', 'key2', 'key3']
        value = 'value1'
        for key in keys:
            yield from cluster.set(key, value)
        res = yield from cluster.keys('*')
        assert set(res) == set(keys)
        print("get_keys -> {}".format(res))
        yield from cluster.clear()  # closing all open connections

    @asyncio.coroutine
    def flush_all(cluster):
        keys = ['key1', 'key2', 'key3']
        value = 'value1'
        for key in keys:
            yield from cluster.set(key, value)
        yield from cluster.flushall()
        res = yield from cluster.keys('*')
        assert [] == res
        print("get_keys after flushall -> {}".format(res))
        yield from cluster.clear()  # closing all open connections

    @asyncio.coroutine
    def scan(cluster):
        yield from cluster.flushall()
        keys = ['key1', 'key2', 'key3']
        value = 'value1'
        for key in keys:
            yield from cluster.set(key, value)
        res = []
        for _keys in (yield from cluster.scan(match='key*')):
            for key in _keys:
                res.append(key)
        assert set(res) == set(keys)
        print("scan -> {}".format(res))
        yield from cluster.clear()  # closing all open connections

    try:
        cluster = loop.run_until_complete(connect())
        for coroutine in (get_key, get_keys, flush_all, scan):
            loop.run_until_complete(coroutine(cluster))
    finally:
        loop.close()


if __name__ == '__main__':
    import sys
    from pathlib import Path
    root = str(Path(__file__).resolve().parents[1])
    sys.path.append(root)
    import asyncio
    from aioredis import create_pool_cluster
    main()
