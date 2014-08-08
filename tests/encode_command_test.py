import unittest

from aioredis.util import encode_command


class EncodeCommandTest(unittest.TestCase):

    def test_encode_bytes(self):
        res = encode_command(b'Hello')
        self.assertEqual(res, b'*1\r\n$5\r\nHello\r\n')

        res = encode_command(b'Hello', b'World')
        self.assertEqual(res, b'*2\r\n$5\r\nHello\r\n$5\r\nWorld\r\n')

        res = encode_command(b'\0')
        self.assertEqual(res, b'*1\r\n$1\r\n\0\r\n')

        res = encode_command(bytearray(b'Hello\r\n'))
        self.assertEqual(res, b'*1\r\n$7\r\nHello\r\n\r\n')

    def test_encode_str(self):
        res = encode_command('Hello')
        self.assertEqual(res, b'*1\r\n$5\r\nHello\r\n')

        res = encode_command('Hello', 'world')
        self.assertEqual(res, b'*2\r\n$5\r\nHello\r\n$5\r\nworld\r\n')

    def test_encode_int(self):
        res = encode_command(1)
        self.assertEqual(res, b'*1\r\n$1\r\n1\r\n')

        res = encode_command(-1)
        self.assertEqual(res, b'*1\r\n$2\r\n-1\r\n')

    def test_encode_float(self):
        res = encode_command(1.0)
        self.assertEqual(res, b'*1\r\n$3\r\n1.0\r\n')

        res = encode_command(-1.0)
        self.assertEqual(res, b'*1\r\n$4\r\n-1.0\r\n')

    def test_encode_empty(self):
        res = encode_command()
        self.assertEqual(res, b'*0\r\n')

    def test_encode_errors(self):
        with self.assertRaises(TypeError):
            encode_command(dict())
        with self.assertRaises(TypeError):
            encode_command(list())
        with self.assertRaises(TypeError):
            encode_command(None)
