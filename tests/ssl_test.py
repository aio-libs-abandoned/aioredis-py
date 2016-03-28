import os
import ssl
import unittest

from ._testutil import run_until_complete, BaseTest


@unittest.skipIf('CERT_FILE' not in os.environ,
                 "CERT_FILE and SSL_PORT parameters expected")
class SSLTest(BaseTest):

    def setUp(self):
        super().setUp()
        cafile = os.environ['CERT_FILE']
        self.ssl_port = int(os.environ.get('SSL_PORT') or 6443)
        if hasattr(ssl, 'create_default_context'):
            # available since python 3.4
            self.ssl_ctx = ssl.create_default_context(cafile=cafile)
        else:
            self.ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
            self.ssl_ctx.load_verify_locations(cafile=cafile)
        if hasattr(self.ssl_ctx, 'check_hostname'):
            # available since python 3.4
            self.ssl_ctx.check_hostname = False
        self.ssl_ctx.verify_mode = ssl.CERT_NONE

    def tearDown(self):
        super().tearDown()
        del self.ssl_ctx

    @run_until_complete
    def test_ssl_connection(self):
        conn = yield from self.create_connection(
            ('localhost', self.ssl_port), ssl=self.ssl_ctx, loop=self.loop)
        res = yield from conn.execute('ping')
        self.assertEqual(res, b'PONG')

    @run_until_complete
    def test_ssl_redis(self):
        redis = yield from self.create_redis(
            ('localhost', self.ssl_port), ssl=self.ssl_ctx, loop=self.loop)
        res = yield from redis.ping()
        self.assertEqual(res, b'PONG')

    @run_until_complete
    def test_ssl_pool(self):
        pool = yield from self.create_pool(
            ('localhost', self.ssl_port), ssl=self.ssl_ctx, loop=self.loop)
        with (yield from pool) as redis:
            res = yield from redis.ping()
            self.assertEqual(res, b'PONG')
