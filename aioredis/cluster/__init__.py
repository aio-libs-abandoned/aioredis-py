from .cluster import (
    RedisPoolCluster,
    create_pool_cluster,
    create_cluster,
    RedisCluster,
)

# make pyflakes happy
(create_pool_cluster, RedisPoolCluster,
 create_cluster, RedisCluster)
