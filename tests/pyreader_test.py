import sys
import pytest

from aioredis.errors import ProtocolError, ReplyError
from aioredis.parser import PyReader


@pytest.fixture
def reader():
    return PyReader()


def test_nothing(reader):
    assert reader.gets() is False


def test_error_when_feeding_non_string(reader):
    with pytest.raises(TypeError):
        reader.feed(1)


def test_protocol_error(reader):
    reader.feed(b"x")
    with pytest.raises(ProtocolError):
        reader.gets()


class CustomExc(Exception):
    pass


@pytest.mark.parametrize('exc,arg', [
    (RuntimeError, RuntimeError),
    (CustomExc, lambda e: CustomExc(e)),
    ], ids=['RuntimeError', 'callable'])
def test_protocol_error_with_custom_class(exc, arg):
    reader = PyReader(protocolError=arg)
    reader.feed(b"x")
    with pytest.raises(exc):
        reader.gets()


@pytest.mark.parametrize('init', [
    dict(protocolError="wrong"),
    dict(replyError="wrong"),
], ids=['wrong protocolError', 'wrong replyError'])
def test_fail_with_wrong_error_class(init):
    with pytest.raises(TypeError):
        PyReader(**init)


def test_error_string(reader):
    reader.feed(b"-error\r\n")
    error = reader.gets()

    assert isinstance(error, ReplyError)
    assert error.args == ("error",)


@pytest.mark.parametrize('exc,arg', [
    (RuntimeError, RuntimeError),
    (CustomExc, lambda e: CustomExc(e)),
    ], ids=['RuntimeError', 'callable'])
def test_error_string_with_custom_class(exc, arg):
    reader = PyReader(replyError=arg)
    reader.feed(b"-error\r\n")
    error = reader.gets()

    assert isinstance(error, exc)
    assert error.args == ("error",)


def test_errors_in_nested_multi_bulk(reader):
    reader.feed(b"*2\r\n-err0\r\n-err1\r\n")

    for r, error in zip(("err0", "err1"), reader.gets()):
        assert isinstance(error, ReplyError)
        assert error.args == (r,)


def test_integer(reader):
    value = 2**63-1  # Largest 64-bit signed integer
    reader.feed((":%d\r\n" % value).encode("ascii"))
    assert reader.gets() == value


def test_status_string(reader):
    reader.feed(b"+ok\r\n")
    assert reader.gets() == b"ok"


def test_empty_bulk_string(reader):
    reader.feed(b"$0\r\n\r\n")
    assert reader.gets() == b''


def test_bulk_string(reader):
    reader.feed(b"$5\r\nhello\r\n")
    assert reader.gets() == b"hello"


def test_bulk_string_without_encoding(reader):
    snowman = b"\xe2\x98\x83"
    reader.feed(b"$3\r\n" + snowman + b"\r\n")
    assert reader.gets() == snowman


@pytest.mark.parametrize('encoding,expected', [
    ('utf-8', b"\xe2\x98\x83".decode('utf-8')),
    ('utf-32', b"\xe2\x98\x83"),
], ids=['utf-8', 'utf-32'])
def test_bulk_string_with_encoding(encoding, expected):
    snowman = b"\xe2\x98\x83"
    reader = PyReader(encoding=encoding)
    reader.feed(b"$3\r\n" + snowman + b"\r\n")
    assert reader.gets() == expected


def test_bulk_string_with_invalid_encoding():
    reader = PyReader(encoding="unknown")
    reader.feed(b"$5\r\nhello\r\n")
    with pytest.raises(LookupError):
        reader.gets()


@pytest.mark.parametrize('data,expected', [
    (b"*-1\r\n", None),
    (b"*0\r\n", []),
    (b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n", [b'hello', b'world']),
], ids=['Null', 'Empty list', 'hello world'])
def test_null_multi_bulk(reader, data, expected):
    reader.feed(data)
    assert reader.gets() == expected


@pytest.mark.xfail
def test_multi_bulk_with_invalid_encoding_and_partial_reply():
    # NOTE: decoding is done when whole value is read
    reader = PyReader(encoding="unknown")
    reader.feed(b"*2\r\n$5\r\nhello\r\n")
    assert reader.gets() is False
    reader.feed(b":1\r\n")
    with pytest.raises(LookupError):
        reader.gets()


def test_nested_multi_bulk(reader):
    reader.feed(b"*2\r\n*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n$1\r\n!\r\n")
    assert reader.gets() == [[b"hello", b"world"], b"!"]


def test_nested_multi_bulk_depth(reader):
    reader.feed(b"*1\r\n*1\r\n*1\r\n*1\r\n$1\r\n!\r\n")
    assert reader.gets() == [[[[b"!"]]]]


def test_invalid_offset(reader):
    data = b"+ok\r\n"
    with pytest.raises(ValueError):
        reader.feed(data, 6)


def test_invalid_length(reader):
    data = b"+ok\r\n"
    with pytest.raises(ValueError):
        reader.feed(data, 0, 6)


def test_ok_offset(reader):
    data = b"blah+ok\r\n"
    reader.feed(data, 4)
    assert reader.gets() == b"ok"


def test_ok_length(reader):
    data = b"blah+ok\r\n"
    reader.feed(data, 4, len(data)-4)
    assert reader.gets() == b"ok"


def test_feed_bytearray(reader):
    if sys.hexversion >= 0x02060000:
        reader.feed(bytearray(b"+ok\r\n"))
        assert reader.gets() == b"ok"


@pytest.mark.xfail()
def test_maxbuf(reader):
    defaultmaxbuf = reader.getmaxbuf()
    reader.setmaxbuf(0)
    assert 0 == reader.getmaxbuf()
    reader.setmaxbuf(10000)
    assert 10000 == reader.getmaxbuf()
    reader.setmaxbuf(None)
    assert defaultmaxbuf == reader.getmaxbuf()
    with pytest.raises(ValueError):
        reader.setmaxbuf(-4)
