import pytest

from aioredis.stream import StreamReader
from aioredis.parser import PyReader
from aioredis.errors import (
    ProtocolError,
    ReplyError
)


@pytest.fixture
def reader(loop):
    reader = StreamReader(loop=loop)
    reader.set_parser(
        PyReader(protocolError=ProtocolError, replyError=ReplyError)
    )
    return reader


@pytest.mark.run_loop
def test_feed_and_parse(reader):
    reader.feed_data(b'+PONG\r\n')
    assert (yield from reader.readobj()) == b'PONG'


@pytest.mark.run_loop
def test_buffer_available_after_RST(reader):
    reader.feed_data(b'+PONG\r\n')
    reader.set_exception(Exception())
    assert (yield from reader.readobj()) == b'PONG'
    with pytest.raises(Exception):
        yield from reader.readobj()


def test_feed_with_eof(reader):
    reader.feed_eof()
    with pytest.raises(AssertionError):
        reader.feed_data(b'+PONG\r\n')


def test_feed_no_data(reader):
    assert not reader.feed_data(None)


@pytest.mark.parametrize(
    'read_method',
    ['read', 'readline', 'readuntil', 'readexactly']
)
@pytest.mark.run_loop
def test_read_flavors_not_supported(reader, read_method):
    with pytest.raises(RuntimeError):
        yield from getattr(reader, read_method)()
