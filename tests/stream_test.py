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


async def test_feed_and_parse(reader):
    reader.feed_data(b'+PONG\r\n')
    assert (await reader.readobj()) == b'PONG'


async def test_buffer_available_after_RST(reader):
    reader.feed_data(b'+PONG\r\n')
    reader.set_exception(Exception())
    assert (await reader.readobj()) == b'PONG'
    with pytest.raises(Exception):
        await reader.readobj()


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
async def test_read_flavors_not_supported(reader, read_method):
    with pytest.raises(RuntimeError):
        await getattr(reader, read_method)()
