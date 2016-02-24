import logging
logging.basicConfig(level=logging.INFO)


NODES = (
    ('172.17.0.2', 7000),
    ('172.17.0.3', 7000),
    ('172.17.0.4', 7000),
)


def main():

    loop = asyncio.get_event_loop()

    async def get_key():
        cluster = await create_pool_cluster(
            NODES, loop=loop, encoding='utf8')
        key = 'key1'
        value = 'value1'
        await  cluster.set(key, value)
        val = await cluster.get(key)
        assert val == value
        print("get value {} -> {}".format(key, value))
        await cluster.clear()  # closing all open connections

    async def get_keys():
        cluster = await create_pool_cluster(
            NODES, loop=loop, encoding='utf8')
        keys = ['key1', 'key2', 'key3']
        value = 'value1'
        for key in keys:
            await cluster.set(key, value)
        val = await cluster.keys()
        assert set(val) == set(keys)
        print("get value {} -> {}".format(val, value))
        await cluster.clear()  # closing all open connections

    async def flash_all():
        cluster = await create_pool_cluster(
            NODES, loop=loop, encoding='utf8')
        keys = ['key1', 'key2', 'key3']
        value = 'value1'
        for key in keys:
            await cluster.set(key, value)
        await cluster.flushall()
        val = await cluster.keys()
        assert [] == val
        print("get value {} -> {}".format(val, value))
        await cluster.clear()  # closing all open connections

    async def scan():
        cluster = await create_pool_cluster(
            NODES, loop=loop, encoding='utf8')
        await cluster.flushall()
        keys = ['key1', 'key2', 'key3']
        value = 'value1'
        for key in keys:
            await cluster.set(key, value)
        res = []
        for _keys in (await cluster.scan(match='key*')):
            for key in _keys:
                res.append(key)
        assert set(res) == set(keys)
        print("get value {} -> {}".format(keys, value))
        await cluster.clear()  # closing all open connections

    try:
        for cor in (get_key(), get_keys(), flash_all(), scan()):
            loop.run_until_complete(cor)
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
