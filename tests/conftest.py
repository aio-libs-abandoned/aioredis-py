import argparse
import asyncio
import random
from typing import Callable, TypeVar
from urllib.parse import urlparse

import pytest
from packaging.version import Version

import aioredis
from aioredis.backoff import NoBackoff
from aioredis.client import Monitor
from aioredis.connection import (
    HIREDIS_AVAILABLE,
    HiredisParser,
    PythonParser,
    parse_url,
)
from aioredis.retry import Retry

from .compat import mock

REDIS_INFO = {}
default_redis_url = "redis://localhost:6379/9"
default_redismod_url = "redis://localhost:36379"

_DecoratedTest = TypeVar("_DecoratedTest", bound="Callable")
_TestDecorator = Callable[[_DecoratedTest], _DecoratedTest]


# Taken from python3.9
class BooleanOptionalAction(argparse.Action):
    def __init__(
        self,
        option_strings,
        dest,
        default=None,
        type=None,
        choices=None,
        required=False,
        help=None,
        metavar=None,
    ):

        _option_strings = []
        for option_string in option_strings:
            _option_strings.append(option_string)

            if option_string.startswith("--"):
                option_string = "--no-" + option_string[2:]
                _option_strings.append(option_string)

        if help is not None and default is not None:
            help += f" (default: {default})"

        super().__init__(
            option_strings=_option_strings,
            dest=dest,
            nargs=0,
            default=default,
            type=type,
            choices=choices,
            required=required,
            help=help,
            metavar=metavar,
        )

    def __call__(self, parser, namespace, values, option_string=None):
        if option_string in self.option_strings:
            setattr(namespace, self.dest, not option_string.startswith("--no-"))

    def format_usage(self):
        return " | ".join(self.option_strings)


def pytest_addoption(parser):
    parser.addoption(
        "--redis-url",
        default=default_redis_url,
        action="store",
        help="Redis connection string, defaults to `%(default)s`",
    )
    parser.addoption(
        "--redismod-url",
        default=default_redismod_url,
        action="store",
        help="Connection string to redis server"
        " with loaded modules,"
        " defaults to `%(default)s`",
    )
    parser.addoption(
        "--uvloop", action=BooleanOptionalAction, help="Run tests with uvloop"
    )


async def _get_info(redis_url):
    client = aioredis.Redis.from_url(redis_url)
    info = await client.info()
    cmds = [c[0].upper().decode() for c in (await client.command())]
    if "dping" in cmds:
        info["enterprise"] = True
    else:
        info["enterprise"] = False
    await client.connection_pool.disconnect()
    return info


def pytest_sessionstart(session):
    use_uvloop = session.config.getoption("--uvloop")

    if use_uvloop:
        try:
            import uvloop

            uvloop.install()
        except ImportError as e:
            raise RuntimeError(
                "Can not import uvloop, make sure it is installed"
            ) from e

    redis_url = session.config.getoption("--redis-url")
    loop = asyncio.get_event_loop()
    info = loop.run_until_complete(_get_info(redis_url))
    version = info["redis_version"]
    arch_bits = info["arch_bits"]
    REDIS_INFO["version"] = version
    REDIS_INFO["arch_bits"] = arch_bits
    REDIS_INFO["enterprise"] = info["enterprise"]

    # module info, if the second redis is running
    try:
        redismod_url = session.config.getoption("--redismod-url")
        info = loop.run_until_complete(_get_info(redismod_url))
        REDIS_INFO["modules"] = info["modules"]
    except aioredis.exceptions.ConnectionError:
        pass
    except KeyError:
        pass


def skip_if_server_version_lt(min_version: str) -> _TestDecorator:
    redis_version = REDIS_INFO["version"]
    check = Version(redis_version) < Version(min_version)
    return pytest.mark.skipif(check, reason=f"Redis version required >= {min_version}")


def skip_if_server_version_gte(min_version: str) -> _TestDecorator:
    redis_version = REDIS_INFO["version"]
    check = Version(redis_version) >= Version(min_version)
    return pytest.mark.skipif(check, reason=f"Redis version required < {min_version}")


def skip_unless_arch_bits(arch_bits: int) -> _TestDecorator:
    return pytest.mark.skipif(
        REDIS_INFO["arch_bits"] != arch_bits,
        reason=f"server is not {arch_bits}-bit",
    )


def skip_ifmodversion_lt(min_version: str, module_name: str) -> _TestDecorator:
    try:
        modules = REDIS_INFO["modules"]
    except KeyError:
        return pytest.mark.skipif(True, reason="Redis server does not have modules")
    if modules == []:
        return pytest.mark.skipif(True, reason="No redis modules found")

    for j in modules:
        if module_name == j.get("name"):
            version = j.get("ver")
            mv = int(min_version.replace(".", ""))
            check = version < mv
            return pytest.mark.skipif(check, reason="Redis module version")

    raise AttributeError(f"No redis module named {module_name}")


def skip_if_redis_enterprise(func) -> _TestDecorator:
    check = REDIS_INFO["enterprise"] is True
    return pytest.mark.skipif(check, reason="Redis enterprise")


def skip_ifnot_redis_enterprise(func) -> _TestDecorator:
    check = REDIS_INFO["enterprise"] is False
    return pytest.mark.skipif(check, reason="Redis enterprise")


@pytest.fixture(
    params=[
        (True, PythonParser),
        (False, PythonParser),
        pytest.param(
            (True, HiredisParser),
            marks=pytest.mark.skipif(
                not HIREDIS_AVAILABLE, reason="hiredis is not installed"
            ),
        ),
        pytest.param(
            (False, HiredisParser),
            marks=pytest.mark.skipif(
                not HIREDIS_AVAILABLE, reason="hiredis is not installed"
            ),
        ),
    ],
    ids=[
        "single-python-parser",
        "pool-python-parser",
        "single-hiredis",
        "pool-hiredis",
    ],
)
def create_redis(request, event_loop):
    """Wrapper around aioredis.create_redis."""
    single_connection, parser_cls = request.param

    async def f(url: str = request.config.getoption("--redis-url"), **kwargs):
        flushdb = kwargs.pop("flushdb", True)
        single = kwargs.pop("single_connection_client", False) or single_connection
        parser_class = kwargs.pop("parser_class", None) or parser_cls
        url_options = parse_url(url)
        url_options.update(kwargs)
        pool = aioredis.ConnectionPool(parser_class=parser_class, **url_options)
        client: aioredis.Redis = aioredis.Redis(connection_pool=pool)
        if single:
            client = client.client()
            await client.initialize()

        def teardown():
            async def ateardown():
                if "username" in kwargs:
                    return
                if flushdb:
                    try:
                        await client.flushdb()
                    except aioredis.ConnectionError:
                        # handle cases where a test disconnected a client
                        # just manually retry the flushdb
                        await client.flushdb()
                await client.close()
                await client.connection_pool.disconnect()

            if event_loop.is_running():
                event_loop.create_task(ateardown())
            else:
                event_loop.run_until_complete(ateardown())

        request.addfinalizer(teardown)

        return client

    return f


# specifically set to the zero database, because creating
# an index on db != 0 raises a ResponseError in redis
@pytest.fixture()
async def modclient(create_redis, request, **kwargs):
    rmurl = request.config.getoption("--redismod-url")
    redis = await create_redis(url=rmurl, decode_responses=True, **kwargs)
    async with redis:
        yield redis


@pytest.fixture()
async def r(create_redis):
    yield await create_redis()


@pytest.fixture()
async def r_timeout(create_redis):

    redis = await create_redis(socket_timeout=1)
    async with redis:
        yield redis


@pytest.fixture()
async def r2(create_redis):
    """A second client for tests that need multiple"""
    yield await create_redis()


def _gen_cluster_mock_resp(r, response):
    connection = mock.AsyncMock()
    connection.retry = Retry(NoBackoff(), 0)
    connection.read_response.return_value = response
    r.connection = connection
    return r


@pytest.fixture()
async def mock_cluster_resp_ok(create_redis, **kwargs):
    r = await create_redis(**kwargs)
    return _gen_cluster_mock_resp(r, "OK")


@pytest.fixture()
async def mock_cluster_resp_int(create_redis, **kwargs):
    r = await create_redis(**kwargs)
    return _gen_cluster_mock_resp(r, "2")


@pytest.fixture()
async def mock_cluster_resp_info(create_redis, **kwargs):
    r = await create_redis(**kwargs)
    response = (
        "cluster_state:ok\r\ncluster_slots_assigned:16384\r\n"
        "cluster_slots_ok:16384\r\ncluster_slots_pfail:0\r\n"
        "cluster_slots_fail:0\r\ncluster_known_nodes:7\r\n"
        "cluster_size:3\r\ncluster_current_epoch:7\r\n"
        "cluster_my_epoch:2\r\ncluster_stats_messages_sent:170262\r\n"
        "cluster_stats_messages_received:105653\r\n"
    )
    return _gen_cluster_mock_resp(r, response)


@pytest.fixture()
async def mock_cluster_resp_nodes(create_redis, **kwargs):
    r = await create_redis(**kwargs)
    response = (
        "c8253bae761cb1ecb2b61857d85dfe455a0fec8b 172.17.0.7:7006 "
        "slave aa90da731f673a99617dfe930306549a09f83a6b 0 "
        "1447836263059 5 connected\n"
        "9bd595fe4821a0e8d6b99d70faa660638a7612b3 172.17.0.7:7008 "
        "master - 0 1447836264065 0 connected\n"
        "aa90da731f673a99617dfe930306549a09f83a6b 172.17.0.7:7003 "
        "myself,master - 0 0 2 connected 5461-10922\n"
        "1df047e5a594f945d82fc140be97a1452bcbf93e 172.17.0.7:7007 "
        "slave 19efe5a631f3296fdf21a5441680f893e8cc96ec 0 "
        "1447836262556 3 connected\n"
        "4ad9a12e63e8f0207025eeba2354bcf4c85e5b22 172.17.0.7:7005 "
        "master - 0 1447836262555 7 connected 0-5460\n"
        "19efe5a631f3296fdf21a5441680f893e8cc96ec 172.17.0.7:7004 "
        "master - 0 1447836263562 3 connected 10923-16383\n"
        "fbb23ed8cfa23f17eaf27ff7d0c410492a1093d6 172.17.0.7:7002 "
        "master,fail - 1447829446956 1447829444948 1 disconnected\n"
    )
    return _gen_cluster_mock_resp(r, response)


@pytest.fixture()
async def mock_cluster_resp_slaves(create_redis, **kwargs):
    r = await create_redis(**kwargs)
    response = (
        "['1df047e5a594f945d82fc140be97a1452bcbf93e 172.17.0.7:7007 "
        "slave 19efe5a631f3296fdf21a5441680f893e8cc96ec 0 "
        "1447836789290 3 connected']"
    )
    return _gen_cluster_mock_resp(r, response)


@pytest.fixture(scope="session")
def master_host(request):
    url = request.config.getoption("--redis-url")
    parts = urlparse(url)
    yield parts.hostname


async def wait_for_command(client: aioredis.Redis, monitor: Monitor, command: str):
    # issue a command with a key name that's local to this process.
    # if we find a command with our key before the command we're waiting
    # for, something went wrong
    redis_version = REDIS_INFO["version"]
    if Version(redis_version) >= Version("5.0.0"):
        id_str = str(await client.client_id())
    else:
        id_str = "%08x" % random.randrange(2 ** 32)
    key = "__REDIS-PY-%s__" % id_str
    await client.get(key)
    while True:
        monitor_response = await monitor.next_command()
        if command in monitor_response["command"]:
            return monitor_response
        if key in monitor_response["command"]:
            return None
