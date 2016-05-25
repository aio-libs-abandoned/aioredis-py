import asyncio
import pytest
import socket
import subprocess
import sys
import re
import contextlib
import os
import ssl
import time

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


@pytest.fixture(scope='session')
def server(start_server):
    """Starts redis-server instance."""
    return start_server('A')


@pytest.fixture(scope='session')
def serverB(start_server):
    """Starts redis-server instance."""
    return start_server('B')


# Internal stuff #


def pytest_addoption(parser):
    parser.addoption('--redis-server', default='/usr/bin/redis-server',
                     help="Path to redis-server executable,"
                          " defaults to `%(default)s`")
    parser.addoption('--ssl-cafile', default='tests/ssl/test.crt',
                     help="Path to testing SSL certificate")


def _read_server_version(config):
    args = [config.getoption('--redis-server'), '--version']
    with subprocess.Popen(args, stdout=subprocess.PIPE) as proc:
        version = proc.stdout.readline().decode('utf-8')
    for part in version.split():
        if part.startswith('v='):
            break
    else:
        raise RuntimeError(
            "No version info can be found in {}".format(version))
    return tuple(map(int, part[2:].split('.')))


@pytest.yield_fixture(scope='session')
def start_server(request, unused_port):

    RedisServer = namedtuple('RedisServer', 'name port unixsocket version')
    processes = []
    servers = {}

    version = _read_server_version(request.config)

    def maker(name):
        if name in servers:
            return servers[name]

        port = unused_port()
        unixsocket = '/tmp/redis.{}.sock'.format(port)

        proc = subprocess.Popen([request.config.getoption('--redis-server'),
                                 '--daemonize', 'no',
                                 '--save', '""',
                                 '--port', str(port),
                                 '--unixsocket', unixsocket,
                                 ], stdout=subprocess.PIPE)
        processes.append(proc)
        log = b''
        while b'The server is now ready to accept connections ' not in log:
            log = proc.stdout.readline()

        info = RedisServer(name, port, unixsocket, version)
        servers.setdefault(name, info)
        return info

    yield maker

    while processes:
        proc = processes.pop(0)
        proc.terminate()
        proc.wait()


@pytest.yield_fixture(scope='session')
def ssl_proxy(request, unused_port):
    processes = []
    by_port = {}

    cafile = os.path.abspath(request.config.getoption('--ssl-cafile'))
    pemfile = os.path.splitext(cafile)[0] + '.pem'
    assert os.path.exists(cafile), \
        "No test ssl certificate, run `make certificate`"
    assert os.path.exists(pemfile), \
        "No test ssl certificate, run `make certificate`"

    if hasattr(ssl, 'create_default_context'):
        ssl_ctx = ssl.create_default_context(cafile=cafile)
    else:
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        ssl_ctx.load_verify_locations(cafile=cafile)
    if hasattr(ssl_ctx, 'check_hostname'):
        # available since python 3.4
        ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    def sockat(unsecure_port):
        if unsecure_port in by_port:
            return by_port[unsecure_port]

        secure_port = unused_port()
        proc = subprocess.Popen(['/usr/bin/socat',
                                 'openssl-listen:{},'
                                 'cert={},'
                                 'verify=0,fork'
                                 .format(secure_port, pemfile),
                                 'tcp-connect:localhost:{}'
                                 .format(unsecure_port)
                                 ])
        processes.append(proc)
        time.sleep(1)   # XXX
        by_port[unsecure_port] = secure_port, ssl_ctx
        return secure_port, ssl_ctx

    yield sockat

    while processes:
        proc = processes.pop(0)
        proc.terminate()
        proc.wait()


@pytest.mark.tryfirst
def pytest_pycollect_makeitem(collector, name, obj):
    if collector.funcnamefilter(name):
        if not callable(obj):
            return
        item = pytest.Function(name, parent=collector)
        if 'run_loop' in item.keywords:
            # TODO: re-wrap with asyncio.coroutine if not native coroutine
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
    if 'py35' in str(path):
        if sys.version_info < (3, 5, 0):
            return True


def pytest_collection_modifyitems(session, config, items):
    version = _read_server_version(config)
    for item in items:
        if 'redis_version' in item.keywords:
            marker = item.keywords['redis_version']
            if version < marker.kwargs['version']:
                item.add_marker(pytest.mark.skip(marker.kwargs['reason']))
        if 'ssl_proxy' in item.fixturenames:
            item.add_marker(pytest.mark.skipif(
                "not os.path.exists('/usr/bin/socat')",
                reason="socat package required (apt-get install socat)"))


@contextlib.contextmanager
def raises_regex(exc_type, message):
    with pytest.raises(exc_type) as exc_info:
        yield exc_info
    match = re.search(message, str(exc_info.value))
    assert match is not None, (
        "Pattern {!r} does not match {!r}"
        .format(message, str(exc_info.value)))


def redis_version(*version, reason):
    assert 1 < len(version) <= 3, version
    assert all(isinstance(v, int) for v in version), version
    return pytest.mark.redis_version(version=version, reason=reason)


def assert_almost_equal(first, second, places=None, msg=None, delta=None):
    assert not (places is None and delta is None)
    if delta is not None:
        assert abs(first - second) <= delta
    else:
        assert round(abs(first - second), places) == 0


def pytest_namespace():
    return {
        'raises_regex': raises_regex,
        'assert_almost_equal': assert_almost_equal,
        'redis_version': redis_version,
        }
