import os
import unittest
import asyncio

from ._testutil import RedisSentinelTest, run_until_complete
import aioredis.errors


"""
This was tested using https://github.com/denverdino/redis-cluster .

It had one local diff:

--- a/redis-config/config.sh
+++ b/redis-config/config.sh
@@ -6,2 +6,4 @@ echo $redismaster_ip

+redis-cli -h $redisslave_ip -p 6379  config set slave-read-only yes
+redis-cli -h $redismaster_ip -p 6379  config set slave-read-only yes
 redis-cli -h $redisslave_ip -p 6379 slaveof $redismaster_ip 6379

"""


@unittest.skipUnless(os.environ.get('SENTINEL'),
                     "Configured to run on travis")
class SentinelTest(RedisSentinelTest):

    @run_until_complete
    def test_sentinel_normal(self):
        key, field, value = b'key:hset', b'bar', b'zap'
        redis = yield from self.get_master_connection()
        exists = yield from redis.hexists(key, field)
        if exists:
            ret = yield from redis.hdel(key, field)
            self.assertEquals(ret, 1)

        ret = yield from redis.hset(key, field, value)
        self.assertEquals(ret, 1)
        ret = yield from redis.hset(key, field, value)
        self.assertEquals(ret, 0)

    @run_until_complete
    def test_sentinel_slave(self):
        key, field, value = b'key:hset', b'bar', b'zap'
        redis = yield from self.get_slave_connection()
        exists = yield from redis.hexists(key, field)
        if exists:
            with self.assertRaises(aioredis.errors.ReadOnlyError):
                yield from redis.hdel(key, field)

        with self.assertRaises(aioredis.errors.ReadOnlyError):
            yield from redis.hset(key, field, value)

    @run_until_complete(600)
    def test_sentinel_slave_fail(self):
        sentinel_connection = self.redis_sentinel.get_sentinel_connection(0)
        key, field, value = b'key:hset', b'bar', b'zap'
        redis = yield from self.get_slave_connection()
        exists = yield from redis.hexists(key, field)
        if exists:
            with self.assertRaises(aioredis.errors.ReadOnlyError):
                yield from redis.hdel(key, field)

        with self.assertRaises(aioredis.errors.ReadOnlyError):
            yield from redis.hset(key, field, value)

        ret = yield from sentinel_connection.sentinel_failover(
            self.sentinel_name)
        self.assertEquals(ret, True)
        yield from asyncio.sleep(2, loop=self.loop)

        with self.assertRaises(aioredis.errors.ReadOnlyError):
            yield from redis.hset(key, field, value)

        ret = yield from sentinel_connection.sentinel_failover(
            self.sentinel_name)
        self.assertEquals(ret, True)
        yield from asyncio.sleep(2, loop=self.loop)
        redis = yield from self.get_slave_connection()
        while True:
            try:
                yield from redis.hset(key, field, value)
                yield from asyncio.sleep(1, loop=self.loop)
                redis = yield from self.get_slave_connection()
            except aioredis.errors.ReadOnlyError:
                break

    @run_until_complete
    def test_sentinel_normal_fail(self):
        sentinel_connection = self.redis_sentinel.get_sentinel_connection(0)
        key, field, value = b'key:hset', b'bar', b'zap'
        redis = yield from self.get_master_connection()
        exists = yield from redis.hexists(key, field)
        if exists:
            ret = yield from redis.hdel(key, field)
            self.assertEquals(ret, 1)

        ret = yield from redis.hset(key, field, value)
        self.assertEquals(ret, 1)
        ret = yield from sentinel_connection.sentinel_failover(
            self.sentinel_name)
        self.assertEquals(ret, True)
        yield from asyncio.sleep(2, loop=self.loop)
        ret = yield from redis.hset(key, field, value)
        self.assertEquals(ret, 0)
        ret = yield from sentinel_connection.sentinel_failover(
            self.sentinel_name)
        self.assertEquals(ret, True)
        yield from asyncio.sleep(2, loop=self.loop)
        redis = yield from self.get_slave_connection()
        while True:
            try:
                yield from redis.hset(key, field, value)
                yield from asyncio.sleep(1, loop=self.loop)
                redis = yield from self.get_slave_connection()
            except aioredis.errors.ReadOnlyError:
                break

    @run_until_complete
    def test_failover(self):
        sentinel_connection = self.redis_sentinel.get_sentinel_connection(0)
        func = sentinel_connection.sentinel_get_master_addr_by_name
        orig_master = yield from func(self.sentinel_name)
        ret = yield from sentinel_connection.sentinel_failover(
            self.sentinel_name)
        self.assertEquals(ret, True)
        yield from asyncio.sleep(2, loop=self.loop)
        new_master = yield from func(self.sentinel_name)
        self.assertNotEquals(orig_master, new_master)
        ret = yield from sentinel_connection.sentinel_failover(
            self.sentinel_name)
        self.assertEquals(ret, True)
        yield from asyncio.sleep(2, loop=self.loop)
        new_master = yield from func(self.sentinel_name)
        self.assertEquals(orig_master, new_master)
        redis = yield from self.get_slave_connection()
        key, field, value = b'key:hset', b'bar', b'zap'
        while True:
            try:
                yield from redis.hset(key, field, value)
                yield from asyncio.sleep(1, loop=self.loop)
                redis = yield from self.get_slave_connection()
            except aioredis.errors.ReadOnlyError:
                break

    @run_until_complete
    def test_get_master(self):
        sentinel_connection = self.redis_sentinel.get_sentinel_connection(0)
        func = sentinel_connection.sentinel_get_master_addr_by_name
        master = yield from func(self.sentinel_name)
        self.assertTrue(isinstance(master, tuple))
        self.assertEquals(len(master), 2)
        self.assertEquals(master[1], 6379)

    @run_until_complete
    def test_get_masters(self):
        sentinel_connection = self.redis_sentinel.get_sentinel_connection(0)
        master = yield from sentinel_connection.sentinel_masters()
        self.assertTrue(isinstance(master, dict))
        self.assertTrue(self.sentinel_name in master)
        master = master[self.sentinel_name]
        self.assertEquals(master['is_slave'], False)
        self.assertEquals(master['name'], self.sentinel_name)
        for k in ['is_master_down', 'num-other-sentinels', 'flags', 'is_odown',
                  'quorum', 'ip', 'failover-timeout', 'runid', 'info-refresh',
                  'config-epoch', 'parallel-syncs', 'role-reported-time',
                  'is_sentinel', 'last-ok-ping-reply',
                  'last-ping-reply', 'last-ping-sent', 'is_sdown', 'is_master',
                  'name', 'pending-commands', 'down-after-milliseconds',
                  'is_slave', 'num-slaves', 'port', 'is_disconnected',
                  'role-reported']:
            self.assertTrue(k in master)

    @run_until_complete
    def test_get_master_info(self):
        sentinel_connection = self.redis_sentinel.get_sentinel_connection(0)
        master = yield from sentinel_connection.sentinel_master(
            self.sentinel_name)
        self.assertTrue(isinstance(master, dict))
        self.assertEquals(master['is_slave'], False)
        self.assertEquals(master['name'], self.sentinel_name)
        for k in ['is_master_down', 'num-other-sentinels', 'flags', 'is_odown',
                  'quorum', 'ip', 'failover-timeout', 'runid', 'info-refresh',
                  'config-epoch', 'parallel-syncs', 'role-reported-time',
                  'is_sentinel', 'last-ok-ping-reply',
                  'last-ping-reply', 'last-ping-sent', 'is_sdown', 'is_master',
                  'name', 'pending-commands', 'down-after-milliseconds',
                  'is_slave', 'num-slaves', 'port', 'is_disconnected',
                  'role-reported']:
            self.assertTrue(k in master)

    @run_until_complete
    def test_get_slave_info(self):
        sentinel_connection = self.redis_sentinel.get_sentinel_connection(0)
        slave = yield from sentinel_connection.sentinel_slaves(
            self.sentinel_name)
        self.assertTrue(len(slave), 1)
        slave = slave[0]
        self.assertTrue(isinstance(slave, dict))
        self.assertEquals(slave['is_slave'], True)
        for k in ['is_master_down', 'flags', 'is_odown',
                  'ip', 'runid', 'info-refresh',
                  'role-reported-time',
                  'is_sentinel', 'last-ok-ping-reply',
                  'last-ping-reply', 'last-ping-sent', 'is_sdown', 'is_master',
                  'name', 'pending-commands', 'down-after-milliseconds',
                  'is_slave', 'port', 'is_disconnected', 'role-reported']:
            self.assertTrue(k in slave, k)

    @run_until_complete
    def test_get_sentinel_info(self):
        sentinel_connection = self.redis_sentinel.get_sentinel_connection(0)
        sentinel = yield from sentinel_connection.sentinel_sentinels(
            self.sentinel_name)
        self.assertEqual(len(sentinel), 0)

    @run_until_complete
    def test_get_sentinel_set_error(self):
        sentinel_connection = self.redis_sentinel.get_sentinel_connection(0)
        with self.assertRaises(aioredis.errors.ReplyError):
            yield from sentinel_connection.sentinel_set(
                self.sentinel_name, 'foo', 'bar')

    @run_until_complete
    def test_get_sentinel_set(self):
        sentinel_connection = self.redis_sentinel.get_sentinel_connection(0)
        resp = yield from sentinel_connection.sentinel_set(self.sentinel_name,
                                                           'failover-timeout',
                                                           1100)
        self.assertEqual(resp, True)
        master = yield from sentinel_connection.sentinel_masters()
        self.assertEqual(master[self.sentinel_name]['failover-timeout'], 1100)

    @run_until_complete
    def test_get_sentinel_monitor(self):
        sentinel_connection = self.redis_sentinel.get_sentinel_connection(0)
        master = yield from sentinel_connection.sentinel_masters()
        if len(master):
            if 'mymaster2' in master:
                resp = yield from sentinel_connection.sentinel_remove(
                    'mymaster2')
                self.assertEqual(resp, True)
        resp = yield from sentinel_connection.sentinel_monitor('mymaster2',
                                                               '127.0.0.1',
                                                               6380, 2)
        self.assertEqual(resp, True)
        resp = yield from sentinel_connection.sentinel_remove('mymaster2')
        self.assertEqual(resp, True)
