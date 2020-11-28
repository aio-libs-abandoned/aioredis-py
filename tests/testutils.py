import asyncio
import functools
from typing import Awaitable, Callable

import pytest

__all__ = ("redis_version", "delay_exc", "select_opener")

from tests.conftest import RedisServer


def redis_version(*version: int, reason: str):
    assert 1 < len(version) <= 3, version
    assert all(isinstance(v, int) for v in version), version
    return pytest.mark.redis_version(version=version, reason=reason)


def delay_exc(fn: Callable[..., Awaitable] = None, *, secs: float):
    def _delayed_exc(func):
        @functools.wraps(func)
        async def _delayed_exc_wrapper(*args, **kwargs):
            await asyncio.sleep(secs)
            return await func(*args, **kwargs)

        return _delayed_exc_wrapper

    return _delayed_exc(fn) if fn else _delayed_exc


def select_opener(connect_type: str, server: RedisServer):
    if connect_type == "tcp":
        from aioredis.connection import open_connection

        return open_connection, server.tcp_address
    if connect_type == "unix":
        from aioredis.connection import open_unix_connection

        return open_unix_connection, server.unixsocket
    raise RuntimeError(f"Connect-type {connect_type!r} is not supported.")
