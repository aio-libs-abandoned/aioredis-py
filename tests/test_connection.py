import asyncio
import types
from typing import TYPE_CHECKING
from unittest import mock

import pytest

from aioredis.connection import UnixDomainSocketConnection
from aioredis.exceptions import InvalidResponse
from aioredis.utils import HIREDIS_AVAILABLE
from tests.conftest import skip_if_server_version_lt

if TYPE_CHECKING:
    from aioredis.connection import PythonParser

pytestmark = pytest.mark.asyncio


@pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
async def test_invalid_response(r):
    raw = b"x"
    parser: "PythonParser" = r.connection._parser
    with mock.patch.object(parser._buffer, "readline", return_value=raw):
        with pytest.raises(InvalidResponse) as cm:
            await parser.read_response()
    assert str(cm.value) == "Protocol Error: %r" % raw


@skip_if_server_version_lt("4.0.0")
@pytest.mark.redismod
async def test_loading_external_modules(modclient):
    def inner():
        pass

    modclient.load_external_module("myfuncname", inner)
    assert getattr(modclient, "myfuncname") == inner
    assert isinstance(getattr(modclient, "myfuncname"), types.FunctionType)

    # and call it
    from aioredis.commands import RedisModuleCommands

    j = RedisModuleCommands.json
    modclient.load_external_module("sometestfuncname", j)

    # d = {'hello': 'world!'}
    # mod = j(modclient)
    # mod.set("fookey", ".", d)
    # assert mod.get('fookey') == d


async def test_socket_param_regression(r):
    """A regression test for issue #1060"""
    conn = UnixDomainSocketConnection()
    await conn.disconnect() == True


async def test_can_run_concurrent_commands(r):
    assert await r.ping() is True
    assert all(await asyncio.gather(*(r.ping() for _ in range(10))))
