import asyncio
import unittest

from ._testutil import RedisSentinelTest, run_until_complete
import aioredis.errors


class SentinelTest(RedisSentinelTest):

    @run_until_complete
    def test_get_master(self):
        master = yield from self.redis.sentinel_get_master_addr_by_name('mymaster')
        self.assertTrue(isinstance(master, tuple))
        self.assertEquals(len(master), 2)
        self.assertEquals(master[1], 6379)

    @run_until_complete
    def test_get_masters(self):
        master = yield from self.redis.sentinel_masters()
        self.assertTrue(isinstance(master, dict))
        self.assertTrue('mymaster' in master)
        master = master['mymaster']
        self.assertEquals(master['is_slave'], False)
        self.assertEquals(master['name'], 'mymaster')
        for k in ['is_master_down', 'num-other-sentinels', 'flags', 'is_odown',
                  'quorum', 'ip', 'failover-timeout', 'runid', 'info-refresh',
                  'config-epoch', 'parallel-syncs', 'role-reported-time',
                  'is_sentinel', 'last-ok-ping-reply',
                  'last-ping-reply', 'last-ping-sent', 'is_sdown', 'is_master',
                  'name', 'pending-commands', 'down-after-milliseconds',
                  'is_slave', 'num-slaves', 'port', 'is_disconnected', 'role-reported']:
            self.assertTrue(k in master)

    @run_until_complete
    def test_get_master_info(self):
        master = yield from self.redis.sentinel_master('mymaster')
        self.assertTrue(isinstance(master, dict))
        self.assertEquals(master['is_slave'], False)
        self.assertEquals(master['name'], 'mymaster')
        for k in ['is_master_down', 'num-other-sentinels', 'flags', 'is_odown',
                  'quorum', 'ip', 'failover-timeout', 'runid', 'info-refresh',
                  'config-epoch', 'parallel-syncs', 'role-reported-time',
                  'is_sentinel', 'last-ok-ping-reply',
                  'last-ping-reply', 'last-ping-sent', 'is_sdown', 'is_master',
                  'name', 'pending-commands', 'down-after-milliseconds',
                  'is_slave', 'num-slaves', 'port', 'is_disconnected', 'role-reported']:
            self.assertTrue(k in master)

    @run_until_complete
    def test_get_slave_info(self):
        slave = yield from self.redis.sentinel_slaves('mymaster')
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
        sentinel = yield from self.redis.sentinel_sentinels('mymaster')
        self.assertEqual(len(sentinel), 0)

    @run_until_complete
    def test_get_sentinel_set_error(self):
        caught = False
        try:
            yield from self.redis.sentinel_set('mymaster', 'foo', 'bar')
        except aioredis.errors.ReplyError:
            caught = True
        self.assertTrue(caught)

    @run_until_complete
    def test_get_sentinel_set(self):
        resp = yield from self.redis.sentinel_set('mymaster', 'failover-timeout', 1100)
        self.assertEqual(resp, True)
        master = yield from self.redis.sentinel_masters()
        self.assertEqual(master['mymaster']['failover-timeout'], 1100)

    @run_until_complete
    def test_get_sentinel_monitor(self):
        master = yield from self.redis.sentinel_masters()
        if len(master):
            if 'mymaster2' in master:
                resp = yield from self.redis.sentinel_remove('mymaster2')
                self.assertEqual(resp, True)
        resp = yield from self.redis.sentinel_monitor('mymaster2', '127.0.0.1', 6380, 2)
        self.assertEqual(resp, True)
        resp = yield from self.redis.sentinel_remove('mymaster2')
        self.assertEqual(resp, True)