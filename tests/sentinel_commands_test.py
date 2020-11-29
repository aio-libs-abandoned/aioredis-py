import asyncio
import logging
import sys
from collections import namedtuple

import pytest

from aioredis import PoolClosedError, RedisError, ReplyError
from aioredis.abc import AbcPool
from aioredis.errors import MasterReplyError
from aioredis.sentinel.commands import RedisSentinel
from tests.testutils import redis_version

pytestmark = redis_version(2, 8, 12, reason="Sentinel v2 required")
if sys.platform == "win32":
    pytestmark = pytest.mark.skip(reason="unstable on windows")

BPO_30399 = sys.version_info >= (3, 7, 0, "alpha", 3)


@pytest.mark.asyncio
async def test_client_close(redis_sentinel):
    assert isinstance(redis_sentinel, RedisSentinel)
    assert not redis_sentinel.closed

    redis_sentinel.close()
    assert redis_sentinel.closed
    with pytest.raises(PoolClosedError):
        assert (await redis_sentinel.ping()) != b"PONG"

    await redis_sentinel.wait_closed()


@pytest.mark.asyncio
async def test_ping(redis_sentinel):
    assert b"PONG" == (await redis_sentinel.ping())


@pytest.mark.asyncio
async def test_master_info(redis_sentinel, sentinel):
    info = await redis_sentinel.master("main-no-fail")
    assert isinstance(info, dict)
    assert info["name"] == "main-no-fail"
    assert "slave" not in info["flags"]
    assert "s_down" not in info["flags"]
    assert "o_down" not in info["flags"]
    assert "sentinel" not in info["flags"]
    assert "disconnected" not in info["flags"]
    assert "master" in info["flags"]

    for key in [
        "num-other-sentinels",
        "flags",
        "quorum",
        "ip",
        "failover-timeout",
        "runid",
        "info-refresh",
        "config-epoch",
        "parallel-syncs",
        "role-reported-time",
        "last-ok-ping-reply",
        "last-ping-reply",
        "last-ping-sent",
        "name",
        "down-after-milliseconds",
        "num-slaves",
        "port",
        "role-reported",
    ]:
        assert key in info
    if sentinel.version < (3, 2, 0):
        assert "pending-commands" in info
    else:
        assert "link-pending-commands" in info
        assert "link-refcount" in info


@pytest.mark.asyncio
async def test_master__auth(create_sentinel, start_sentinel, start_server):
    main = start_server("main_1", password="123")
    start_server("replica_1", slaveof=main, password="123")

    sentinel = start_sentinel("auth_sentinel_1", main)
    client1 = await create_sentinel([sentinel.tcp_address], password="123", timeout=1)

    client2 = await create_sentinel([sentinel.tcp_address], password="111", timeout=1)

    client3 = await create_sentinel([sentinel.tcp_address], timeout=1)

    m1 = client1.master_for(main.name)
    await m1.set("mykey", "myval")

    with pytest.raises(MasterReplyError) as exc_info:
        m2 = client2.master_for(main.name)
        await m2.set("mykey", "myval")
    if BPO_30399:
        expected = "('Service main_1 error', AuthError('ERR invalid password'))"
    else:
        expected = "('Service main_1 error', AuthError('ERR invalid password',))"
    assert str(exc_info.value) == expected

    with pytest.raises(MasterReplyError):
        m3 = client3.master_for(main.name)
        await m3.set("mykey", "myval")


@pytest.mark.asyncio
async def test_master__no_auth(create_sentinel, sentinel):
    client = await create_sentinel([sentinel.tcp_address], password="123", timeout=1)

    main = client.master_for("mainA")
    with pytest.raises(MasterReplyError):
        await main.set("mykey", "myval")


@pytest.mark.asyncio
async def test_master__unknown(redis_sentinel):
    with pytest.raises(ReplyError):
        await redis_sentinel.master("unknown-main")


@pytest.mark.asyncio
async def test_master_address(redis_sentinel, sentinel):
    _, port = await redis_sentinel.master_address("main-no-fail")
    assert port == sentinel.masters["main-no-fail"].tcp_address.port


@pytest.mark.asyncio
async def test_master_address__unknown(redis_sentinel):
    res = await redis_sentinel.master_address("unknown-main")
    assert res is None


@pytest.mark.asyncio
async def test_masters(redis_sentinel):
    masters = await redis_sentinel.masters()
    assert isinstance(masters, dict)
    assert len(masters) >= 1, "At least on masters expected"
    assert "main-no-fail" in masters
    assert isinstance(masters["main-no-fail"], dict)


@pytest.mark.asyncio
async def test_slave_info(sentinel, redis_sentinel):
    info = await redis_sentinel.slaves("main-no-fail")
    assert len(info) == 1
    info = info[0]
    assert isinstance(info, dict)
    assert "master" not in info["flags"]
    assert "s_down" not in info["flags"]
    assert "o_down" not in info["flags"]
    assert "sentinel" not in info["flags"]
    # assert 'disconnected' not in info['flags']
    assert "slave" in info["flags"]

    keys_set = {
        "flags",
        "master-host",
        "master-link-down-time",
        "master-link-status",
        "master-port",
        "name",
        "slave-priority",
        "ip",
        "runid",
        "info-refresh",
        "role-reported-time",
        "last-ok-ping-reply",
        "last-ping-reply",
        "last-ping-sent",
        "down-after-milliseconds",
        "port",
        "role-reported",
    }
    if sentinel.version < (3, 2, 0):
        keys_set.add("pending-commands")
    else:
        keys_set.add("link-pending-commands")
        keys_set.add("link-refcount")

    missing = keys_set - set(info)
    assert not missing


@pytest.mark.asyncio
async def test_slave__unknown(redis_sentinel):
    with pytest.raises(ReplyError):
        await redis_sentinel.slaves("unknown-main")


@pytest.mark.asyncio
async def test_sentinels_empty(redis_sentinel):
    res = await redis_sentinel.sentinels("main-no-fail")
    assert res == []

    with pytest.raises(ReplyError):
        await redis_sentinel.sentinels("unknown-main")


@pytest.mark.timeout(30)
@pytest.mark.asyncio
async def test_sentinels__exist(create_sentinel, start_sentinel, start_server):
    m1 = start_server("main-two-sentinels")
    s1 = start_sentinel("peer-sentinel-1", m1, quorum=2, noslaves=True)
    s2 = start_sentinel("peer-sentinel-2", m1, quorum=2, noslaves=True)

    redis_sentinel = await create_sentinel([s1.tcp_address, s2.tcp_address], timeout=1)

    while True:
        info = await redis_sentinel.master("main-two-sentinels")
        if info["num-other-sentinels"] > 0:
            break
        await asyncio.sleep(0.2)
    info = await redis_sentinel.sentinels("main-two-sentinels")
    assert len(info) == 1
    assert "sentinel" in info[0]["flags"]
    assert info[0]["port"] in (s1.tcp_address.port, s2.tcp_address.port)


@pytest.mark.asyncio
async def test_ckquorum(redis_sentinel):
    assert await redis_sentinel.check_quorum("main-no-fail")

    # change quorum

    assert await redis_sentinel.set("main-no-fail", "quorum", 2)

    with pytest.raises(RedisError):
        await redis_sentinel.check_quorum("main-no-fail")

    assert await redis_sentinel.set("main-no-fail", "quorum", 1)
    assert await redis_sentinel.check_quorum("main-no-fail")


@pytest.mark.asyncio
async def test_set_option(redis_sentinel):
    assert await redis_sentinel.set("main-no-fail", "quorum", 10)
    main = await redis_sentinel.master("main-no-fail")
    assert main["quorum"] == 10

    assert await redis_sentinel.set("main-no-fail", "quorum", 1)
    main = await redis_sentinel.master("main-no-fail")
    assert main["quorum"] == 1

    with pytest.raises(ReplyError):
        await redis_sentinel.set("mainA", "foo", "bar")


@pytest.mark.asyncio
async def test_sentinel_role(sentinel, create_redis):
    redis = await create_redis(sentinel.tcp_address)
    info = await redis.role()
    assert info.role == "sentinel"
    assert isinstance(info.masters, list)
    assert "main-no-fail" in info.masters


@pytest.mark.timeout(30)
@pytest.mark.asyncio
async def test_remove(redis_sentinel, start_server):
    m1 = start_server("main-to-remove")
    ok = await redis_sentinel.monitor(m1.name, "127.0.0.1", m1.tcp_address.port, 1)
    assert ok

    ok = await redis_sentinel.remove(m1.name)
    assert ok

    with pytest.raises(ReplyError):
        await redis_sentinel.remove("unknown-main")


@pytest.mark.timeout(30)
@pytest.mark.asyncio
async def test_monitor(redis_sentinel, start_server, unused_tcp_port):
    m1 = start_server("main-to-monitor")
    ok = await redis_sentinel.monitor(m1.name, "127.0.0.1", m1.tcp_address.port, 1)
    assert ok

    _, port = await redis_sentinel.master_address("main-to-monitor")
    assert port == m1.tcp_address.port


@pytest.mark.timeout(5)
@pytest.mark.asyncio
async def test_sentinel_master_pool_size(sentinel, create_sentinel, caplog):
    redis_s = await create_sentinel(
        [sentinel.tcp_address], timeout=1, minsize=10, maxsize=10
    )
    main = redis_s.master_for("main-no-fail")
    assert isinstance(main.connection, AbcPool)
    assert main.connection.size == 0

    caplog.clear()
    with caplog.at_level("DEBUG", "aioredis.sentinel"):
        assert await main.ping()
    assert len(caplog.record_tuples) == 1
    assert caplog.record_tuples == [
        (
            "aioredis.sentinel",
            logging.DEBUG,
            f"Discoverred new address {main.address} for main-no-fail",
        ),
    ]
    assert main.connection.size == 10
    assert main.connection.freesize == 10


@pytest.mark.asyncio
async def test_sentinel_no_discovered_pool(tcp_port_factory, create_sentinel):
    tcp_address = namedtuple("TCPAddress", "host port")
    port = tcp_port_factory()
    addr = tcp_address("localhost", port)

    with pytest.raises(Exception):
        await create_sentinel([addr], timeout=2)


@pytest.mark.asyncio
async def test_sentinel_with_password(start_server, start_sentinel, create_sentinel):
    password = "qwert"
    n = start_server("redis-normal")
    if n.version[0] < 3:
        pytest.mark.skip(reason="Redis does not support sentinel password before version 3")
        return

    s = start_sentinel("sentinel-normal", n, sentinel_password=password)
    client = await create_sentinel([s.tcp_address], sentinel_password=password, timeout=10)
    assert await client.ping() == b"PONG"
