from __future__ import annotations

import asyncio

import pytest

import aioredis
from aioredis.connection import DefaultReader, UnixDomainSocketConnection
from aioredis.exceptions import InvalidResponse
from aioredis.parser import PythonReader


@pytest.mark.asyncio
@pytest.mark.skip(
    reason="TODO: Hangs forever on teardown. Reader/Connection in bad state."
)
@pytest.mark.parametrize("create_redis", [(True, PythonReader)], indirect=True)
async def test_invalid_response(create_redis):
    r: aioredis.Redis = await create_redis()

    raw = b"x"

    reader: DefaultReader = r.connection._protocol._parser
    reader.feed(raw)
    with pytest.raises(InvalidResponse) as cm:
        reader.gets()
    await r.close(close_connection_pool=True)
    assert str(cm.value) == "Protocol Error: %r" % raw.decode()


@pytest.mark.asyncio
async def test_socket_param_regression(r):
    """A regression test for issue #1060"""
    conn = UnixDomainSocketConnection()
    assert await conn.disconnect() == True


@pytest.mark.asyncio
async def test_can_run_concurrent_commands(r):
    assert await r.ping() is True
    assert all(await asyncio.gather(*(r.ping() for _ in range(10))))
