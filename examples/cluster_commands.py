import logging


from aioredis.errors import RedisClusterError
from pprint import pprint

logging.basicConfig(level=logging.INFO)

NODES = [('localhost', port) for port in range(7001, 7007)]


def main():

    loop = asyncio.get_event_loop()

    async def connect():
        try:
            return await create_cluster(NODES, loop=loop, encoding='utf8')
        except RedisClusterError:
            raise RedisClusterError(
                "Could not connect to cluster. Did you start it with "
                "the setupcluster.py script?"
            )

    async def cluster_info(cluster):
        info = await cluster.cluster_info()
        pprint(info)

    async def cluster_nodes(cluster):
        for node in await cluster.cluster_nodes():
            pprint(node)

    async def cluster_slots(cluster):
        pprint(await cluster.cluster_slots())

    async def cluster_slaves(cluster):
        for node in await cluster.cluster_nodes():
            if 'slave' in node['flags']:  # Slave node will return error
                continue

            slaves = list(await cluster.cluster_slaves(node['id']))
            pprint(slaves)

    async def cluster_slots_del_add(cluster):
        node = cluster.get_node('x')

        # It will determine node and remove slot
        await cluster.cluster_del_slots(node.slots[0][0] + 1)

        # Checking node for deleted slot
        pprint(await cluster.cluster_slots(address=node.address))

        # Returning slot to exact node
        await cluster.cluster_add_slots(
            node.slots[0][0] + 1, address=node.address
        )

    try:
        cluster = loop.run_until_complete(connect())
        for coroutine in (
            cluster_info, cluster_nodes, cluster_slots, cluster_slaves,
            cluster_slots_del_add
        ):
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
