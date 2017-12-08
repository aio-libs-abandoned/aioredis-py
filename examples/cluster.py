import logging

from aioredis.errors import RedisClusterError

logging.basicConfig(level=logging.INFO)

NODES = [('localhost', port) for port in range(7001, 7007)]


def main():

    loop = asyncio.get_event_loop()

    async def connect():
        try:
            return await  create_cluster(NODES, loop=loop, encoding='utf8')
        except RedisClusterError:
            raise RedisClusterError(
                "Could not connect to cluster. Did you start it with "
                "the setupcluster.py script?"
            )

    async def get_key(cluster):
        key = 'key1'
        value = 'value1'
        await cluster.set(key, value)
        res = await cluster.get(key)
        assert res == value
        print("get_key {} -> {}".format(key, res))
        await cluster.clear()  # closing all open connections

    async def get_keys(cluster):
        keys = ['key1', 'key2', 'key3']
        value = 'value1'
        for key in keys:
            await cluster.set(key, value)
        res = await cluster.keys('*')

        assert set(keys).issubset(set(res))
        print("get_keys -> {}".format(res))
        await cluster.clear()  # closing all open connections

    async def flush_all(cluster):
        keys = ['key1', 'key2', 'key3']
        value = 'value1'
        for key in keys:
            await cluster.set(key, value)
        await cluster.flushall()
        res = await cluster.keys('*')
        assert [] == res
        print("get_keys after flushall -> {}".format(res))
        await cluster.clear()  # closing all open connections

    async def scan(cluster):
        await cluster.flushall()
        keys = ['key1', 'key2', 'key3']
        value = 'value1'
        for key in keys:
            await cluster.set(key, value)

        res = await cluster.scan(match='key*')
        assert set(keys).issubset(set(res))
        print("scan -> {}".format(res))
        await cluster.clear()  # closing all open connections

    try:
        cluster = loop.run_until_complete(connect())
        for coroutine in (get_key, get_keys, flush_all, scan):
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
