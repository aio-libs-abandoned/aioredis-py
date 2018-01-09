from aioredis.cluster.testcluster import TestCluster

cluster = TestCluster(list(range(7001, 7007)), '/tmp/rediscluster')
cluster.setup()