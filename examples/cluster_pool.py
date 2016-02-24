import logging
logging.basicConfig(level=logging.INFO)


NODES = (
    ('172.17.0.2', 7000),
    ('172.17.0.3', 7000),
    ('172.17.0.4', 7000),
)


def main():

    loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def get_key():
        cluster = yield from create_pool_cluster(
            NODES, loop=loop, encoding='utf8')
        key = 'key1'
        value = 'value1'
        yield from cluster.set(key, value)
        val = yield from cluster.get(key)
        assert val == value
        print("get value {} -> {}".format(key, value))
        yield from cluster.clear()  # closing all open connections

    @asyncio.coroutine
    def get_keys():
        cluster = yield from create_pool_cluster(
            NODES, loop=loop, encoding='utf8')
        keys = ['key1', 'key2', 'key3']
        value = 'value1'
        for key in keys:
            yield from cluster.set(key, value)
        val = yield from cluster.keys()
        assert set(val) == set(keys)
        print("get value {} -> {}".format(val, value))
        yield from cluster.clear()  # closing all open connections

    @asyncio.coroutine
    def flash_all():
        cluster = yield from create_pool_cluster(
            NODES, loop=loop, encoding='utf8')
        keys = ['key1', 'key2', 'key3']
        value = 'value1'
        for key in keys:
            yield from cluster.set(key, value)
        yield from cluster.flushall()
        val = yield from cluster.keys()
        assert [] == val
        print("get value {} -> {}".format(val, value))
        yield from cluster.clear()  # closing all open connections

    @asyncio.coroutine
    def scan():
        cluster = yield from create_pool_cluster(
            NODES, loop=loop, encoding='utf8')
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
        print("get value {} -> {}".format(keys, value))
        yield from cluster.clear()  # closing all open connections
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
