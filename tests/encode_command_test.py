import pytest

from aioredis.util import encode_command


def test_encode_bytes():
    res = encode_command(b'Hello')
    assert res == b'*1\r\n$5\r\nHello\r\n'

    res = encode_command(b'Hello', b'World')
    assert res == b'*2\r\n$5\r\nHello\r\n$5\r\nWorld\r\n'

    res = encode_command(b'\0')
    assert res == b'*1\r\n$1\r\n\0\r\n'

    res = encode_command(bytearray(b'Hello\r\n'))
    assert res == b'*1\r\n$7\r\nHello\r\n\r\n'


def test_encode_str():
    res = encode_command('Hello')
    assert res == b'*1\r\n$5\r\nHello\r\n'

    res = encode_command('Hello', 'world')
    assert res == b'*2\r\n$5\r\nHello\r\n$5\r\nworld\r\n'


def test_encode_int():
    res = encode_command(1)
    assert res == b'*1\r\n$1\r\n1\r\n'

    res = encode_command(-1)
    assert res == b'*1\r\n$2\r\n-1\r\n'


def test_encode_float():
    res = encode_command(1.0)
    assert res == b'*1\r\n$3\r\n1.0\r\n'

    res = encode_command(-1.0)
    assert res == b'*1\r\n$4\r\n-1.0\r\n'


def test_encode_empty():
    res = encode_command()
    assert res == b'*0\r\n'


def test_encode_errors():
    with pytest.raises(TypeError):
        encode_command(dict())
    with pytest.raises(TypeError):
        encode_command(list())
    with pytest.raises(TypeError):
        encode_command(None)
