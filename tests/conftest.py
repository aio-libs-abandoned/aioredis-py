import asyncio
import pytest
import socket
import subprocess
import os
import sys

from collections import namedtuple

import aioredis


# Public fixtures


@pytest.yield_fixture
def loop():
    """Creates new event loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(None)

    yield loop

    if hasattr(loop, 'is_closed'):
        closed = loop.is_closed()
    else:
        closed = loop._closed   # XXX
    if not closed:
        loop.call_soon(loop.stop)
        loop.run_forever()
        loop.close()


@pytest.fixture(scope='session')
def unused_port():
    """Gets random free port."""
    def fun():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            return s.getsockname()[1]
    return fun


@pytest.fixture
def create_connection(_closable):
    """Wrapper around aioredis.create_connection."""

    @asyncio.coroutine
    def f(*args, **kw):
        conn = yield from aioredis.create_connection(*args, **kw)
        _closable(conn)
        return conn
    return f


@pytest.fixture
def create_redis(_closable, loop):
    """Wrapper around aioredis.create_redis."""

    @asyncio.coroutine
    def f(*args, **kw):
        redis = yield from aioredis.create_redis(*args, **kw)
        _closable(redis)
        return redis
    return f


@pytest.fixture
def create_pool(_closable):
    """Wrapper around aioredis.create_pool."""

    @asyncio.coroutine
    def f(*args, **kw):
        redis = yield from aioredis.create_pool(*args, **kw)
        _closable(redis)
        return redis
    return f


@pytest.fixture
def redis(create_redis, server, loop):
    """Returns Redis client instance."""
    redis = loop.run_until_complete(
        create_redis(('localhost', server.port), loop=loop))
    loop.run_until_complete(redis.flushall())
    return redis


@pytest.yield_fixture
def _closable(loop):
    conns = []

    yield conns.append

    waiters = []
    while conns:
        conn = conns.pop(0)
        # XXX: make pool have same close/wait_closed interface
        if isinstance(conn, (aioredis.RedisConnection, aioredis.Redis)):
            conn.close()
            waiters.append(conn.wait_closed())
        else:
            waiters.append(conn.clear())
    if waiters:
        loop.run_until_complete(asyncio.gather(*waiters, loop=loop))

# Internal stuff #


def pytest_addoption(parser):
    parser.addoption('--redis-server', default='/usr/bin/redis-server',
                     help="Path to redis-server executable,"
                          " defaults to `%(default)s`")


RedisServer = namedtuple('RedisServer', 'port unixsocket')


@pytest.yield_fixture(scope='session')
def server(request, unused_port):
    """Starts redis-server instance."""

    port = unused_port()
    unixsocket = '/tmp/redis.{}.sock'.format(port)

    args = [request.config.getoption('--redis-server'),
            '--daemonize', 'no',
            '--save', '""',
            '--port', str(port),
            '--unixsocket', unixsocket,
            ]
    with subprocess.Popen(args, stdout=subprocess.PIPE) as proc:
        log = b''
        while b'The server is now ready to accept connections ' not in log:
            log = proc.stdout.readline()
        yield RedisServer(port, unixsocket)
        proc.terminate()
        os.remove(unixsocket)


@pytest.mark.tryfirst
def pytest_pycollect_makeitem(collector, name, obj):
    if collector.funcnamefilter(name):
        if not callable(obj):
            return
        item = pytest.Function(name, parent=collector)
        if 'run_loop' in item.keywords:
            return list(collector._genfunctions(name, obj))


@pytest.mark.tryfirst
def pytest_pyfunc_call(pyfuncitem):
    """
    Run asyncio marked test functions in an event loop instead of a normal
    function call.
    """
    if 'run_loop' in pyfuncitem.keywords:
        funcargs = pyfuncitem.funcargs
        loop = funcargs['loop']
        testargs = {arg: funcargs[arg]
                    for arg in pyfuncitem._fixtureinfo.argnames}
        loop.run_until_complete(
            asyncio.wait_for(pyfuncitem.obj(**testargs),
                             15, loop=loop))
        return True


def pytest_runtest_setup(item):
    if 'run_loop' in item.keywords and 'loop' not in item.fixturenames:
        # inject an event loop fixture for all async tests
        item.fixturenames.append('loop')


def pytest_ignore_collect(path, config):
    if 'test_py35' in str(path):
        if sys.version_info < (3, 5, 0):
            return True


def pytest_collection_modifyitems(session, config, items):
    # Run and parse redis-server --version
    # TODO: make it use pytest_namespace()

    args = [config.getoption('--redis-server'), '--version']
    with subprocess.Popen(args, stdout=subprocess.PIPE) as proc:
        version = proc.stdout.readline().decode('utf-8')
    for part in version.split():
        if part.startswith('v='):
            break
    else:
        return
    version = tuple(map(int, part[2:].split('.')))
    for item in items:
        if 'redis_version' not in item.keywords:
            continue
        marker = item.keywords['redis_version']
        assert all(isinstance(a, int) for a in marker.args), marker.args
        if version < marker.args:
            reason = 'Expects server version {}, got {}'.format(
                marker.args, version)
            reason = marker.kwargs.get('reason', reason)
            item.add_marker(pytest.mark.skip(reason))
