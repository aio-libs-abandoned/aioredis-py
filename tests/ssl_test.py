import os
import ssl
import pytest


pytestmark = pytest.mark.skipif(
    'CERT_FILE' not in os.environ,
    reason="CERT_FILE and SSL_PORT parameters expected")


def make_ctx():
    cafile = os.environ['CERT_FILE']
    if hasattr(ssl, 'create_default_context'):
        # available since python 3.4
        ssl_ctx = ssl.create_default_context(cafile=cafile)
    else:
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        ssl_ctx.load_verify_locations(cafile=cafile)
    if hasattr(ssl_ctx, 'check_hostname'):
        # available since python 3.4
        ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    return ssl_ctx


@pytest.mark.run_loop
def test_ssl_connection(create_connection, loop, server):
    conn = yield from create_connection(
        ('localhost', server.ssl_port), ssl=make_ctx(), loop=loop)
    res = yield from conn.execute('ping')
    assert res == b'PONG'


@pytest.mark.run_loop
def test_ssl_redis(create_redis, loop, server):
    redis = yield from create_redis(
        ('localhost', server.ssl_port), ssl=make_ctx(), loop=loop)
    res = yield from redis.ping()
    assert res == b'PONG'


@pytest.mark.run_loop
def test_ssl_pool(create_pool, server, loop):
    pool = yield from create_pool(
        ('localhost', server.ssl_port), ssl=make_ctx(), loop=loop)
    with (yield from pool) as redis:
        res = yield from redis.ping()
        assert res == b'PONG'
