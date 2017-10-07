from collections import OrderedDict

import os
import sys

import pytest
import asyncio

STREAMS_AVAILABLE = (
    os.environ.get('STREAMS_AVAILABLE')
    # Should be true in CI when building the streams branch:
    or 'streams' in ' '.join(sys.argv)
)
SKIP_REASON = (
    "Streams testing is disabled as streams are not yet available "
    "in Redis 4.0. Set STREAMS_AVAILABLE=1 in your environment "
    "if you have compiled the Redis 'streams' branch. You will "
    "probably need specify the --redis-server=path/to/redis-server "
    " to py.test."
)


@asyncio.coroutine
def add_message_with_sleep(redis, loop, stream, fields):
    yield from asyncio.sleep(0.2, loop=loop)
    result = yield from redis.xadd(stream, fields)
    return result


@pytest.mark.skipif(not STREAMS_AVAILABLE, reason=SKIP_REASON)
@pytest.mark.run_loop
def test_xadd(redis):
    values = {
        'field1': 'value1',
        'field2': 'value2',
    }
    message_id = yield from redis.xadd('test_stream', values)

    # Check the result is in the expected format (i.e: 1507400517949-0)
    assert b'-' in message_id
    timestamp, sequence = message_id.split(b'-')
    assert timestamp.isdigit()
    assert sequence.isdigit()

    # Read it back
    messages = yield from redis.xrange('test_stream')
    assert len(messages) == 1
    message = messages[0]
    assert message[0] == message_id
    assert message[1] == OrderedDict([
        (b'field1', b'value1'),
        (b'field2', b'value2')]
    )


@pytest.mark.skipif(not STREAMS_AVAILABLE, reason=SKIP_REASON)
@pytest.mark.run_loop
def test_xrange(redis):
    stream = 'test_stream'
    values = {
        'field1': 'value1',
        'field2': 'value2',
    }
    message_id1 = yield from redis.xadd(stream, values)
    message_id2 = yield from redis.xadd(stream, values)
    message_id3 = yield from redis.xadd(stream, values)  # noqa

    # Test no parameters
    messages = yield from redis.xrange(stream)
    assert len(messages) == 3
    message = messages[0]
    assert message[0] == message_id1
    assert message[1] == OrderedDict([
        (b'field1', b'value1'),
        (b'field2', b'value2')]
    )

    # Test start
    messages = yield from redis.xrange(stream, start=message_id2)
    assert len(messages) == 2

    messages = yield from redis.xrange(stream, start='9900000000000-0')
    assert len(messages) == 0

    # Test stop
    messages = yield from redis.xrange(stream, stop='0000000000000-0')
    assert len(messages) == 0

    messages = yield from redis.xrange(stream, stop=message_id2)
    assert len(messages) == 2

    messages = yield from redis.xrange(stream, stop='9900000000000-0')
    assert len(messages) == 3

    # Test start & stop
    messages = yield from redis.xrange(stream,
                                       start=message_id1,
                                       stop=message_id2)
    assert len(messages) == 2

    messages = yield from redis.xrange(stream,
                                       start='0000000000000-0',
                                       stop='9900000000000-0')
    assert len(messages) == 3

    # Test count
    messages = yield from redis.xrange(stream, count=2)
    assert len(messages) == 2


@pytest.mark.skipif(not STREAMS_AVAILABLE, reason=SKIP_REASON)
@pytest.mark.run_loop
def test_xread_selection(redis, create_redis, loop, server):
    """Test use of counts and starting IDs"""
    stream = 'test_stream'
    values = {
        'field1': 'value1',
        'field2': 'value2',
    }
    message_id1 = yield from redis.xadd(stream, values)
    message_id2 = yield from redis.xadd(stream, values)  # noqa
    message_id3 = yield from redis.xadd(stream, values)

    messages = yield from redis.xread([stream],
                                      timeout=1,
                                      latest_ids=['0000000000000-0'])
    assert len(messages) == 3

    messages = yield from redis.xread([stream],
                                      timeout=1,
                                      latest_ids=[message_id1])
    assert len(messages) == 2

    messages = yield from redis.xread([stream],
                                      timeout=1,
                                      latest_ids=[message_id3])
    assert len(messages) == 0

    messages = yield from redis.xread([stream],
                                      timeout=1,
                                      latest_ids=['0000000000000-0'], count=2)
    assert len(messages) == 2


@pytest.mark.skipif(not STREAMS_AVAILABLE, reason=SKIP_REASON)
@pytest.mark.run_loop
def test_xread_blocking(redis, create_redis, loop, server, server_bin):
    """Test the blocking read features"""
    fields = OrderedDict((
        (b'field1', b'value1'),
        (b'field2', b'value2'),
    ))
    other_redis = yield from create_redis(
        server.tcp_address, loop=loop)

    # create blocking task in separate connection
    consumer = other_redis.xread(['test_stream'], timeout=1000)

    producer_task = asyncio.Task(
        add_message_with_sleep(redis, loop, 'test_stream', fields), loop=loop)
    results = yield from asyncio.gather(
        consumer, producer_task, loop=loop)

    received_messages, sent_message_id = results
    assert len(received_messages) == 1
    assert sent_message_id

    received_stream, received_message_id, received_fields \
        = received_messages[0]

    assert received_stream == b'test_stream'
    assert sent_message_id == received_message_id
    assert fields == received_fields

    # Test that we get nothing back from an empty stream
    results = yield from redis.xread(['another_stream'], timeout=100)
    assert results == []

    other_redis.close()
