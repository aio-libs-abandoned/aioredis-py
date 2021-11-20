import asyncio
from typing import TYPE_CHECKING
from unittest import mock

import pytest

from aioredis.connection import PythonParser, UnixDomainSocketConnection
from aioredis.exceptions import InvalidResponse


@pytest.mark.asyncio
@pytest.mark.parametrize("create_redis", [(True, PythonParser)], indirect=True)
async def test_invalid_response(create_redis):
    r = await create_redis()

    raw = b"x"
    parser: "PythonParser" = r.connection._parser
    with mock.patch.object(parser._buffer, "readline", return_value=raw):
        with pytest.raises(InvalidResponse) as cm:
            await parser.read_response()
    assert str(cm.value) == "Protocol Error: %r" % raw


@pytest.mark.asyncio
async def test_socket_param_regression(r):
    """A regression test for issue #1060"""
    conn = UnixDomainSocketConnection()
    await conn.disconnect() == True


@pytest.mark.asyncio
async def test_can_run_concurrent_commands(r):
    assert await r.ping() is True
    assert all(await asyncio.gather(*(r.ping() for _ in range(10))))
