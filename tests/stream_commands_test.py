from collections import OrderedDict

from time import sleep

import pytest
import asyncio


@asyncio.coroutine
async def add_message_with_sleep(redis, loop, stream, fields):
    await asyncio.sleep(0.2, loop=loop)
    result = await redis.xadd(stream, fields)
    return result


@pytest.mark.run_loop
@pytest.redis_version(999, 999, 999, reason="Streams only available on redis "
                                            "unstable branch")
async def test_xadd(redis, server_bin):
    fields = OrderedDict((
        (b'field1', b'value1'),
        (b'field2', b'value2'),
    ))
    message_id = await redis.xadd('test_stream', fields)

    # Check the result is in the expected format (i.e: 1507400517949-0)
    assert b'-' in message_id
    timestamp, sequence = message_id.split(b'-')
    assert timestamp.isdigit()
    assert sequence.isdigit()

    # Read it back
    messages = await redis.xrange('test_stream')
    assert len(messages) == 1
    message = messages[0]
    assert message[0] == message_id
    assert message[1] == OrderedDict([
        (b'field1', b'value1'),
        (b'field2', b'value2')]
    )


@pytest.mark.run_loop
@pytest.redis_version(999, 999, 999, reason="Streams only available on redis "
                                            "unstable branch")
async def test_xadd_maxlen_exact(redis, server_bin):
    message_id1 = await redis.xadd('test_stream', {'f1': 'v1'})  # noqa
    sleep(0.001)  # Ensure the millisecond-based message ID increments
    message_id2 = await redis.xadd('test_stream', {'f2': 'v2'})
    sleep(0.001)
    message_id3 = await redis.xadd('test_stream', {'f3': 'v3'},
                                   max_len=2, exact_len=True)

    # Read it back
    messages = await redis.xrange('test_stream')
    assert len(messages) == 2

    message2 = messages[0]
    message3 = messages[1]

    # The first message should no longer exist, just messages
    # 2 and 3 remain
    assert message2[0] == message_id2
    assert message2[1] == OrderedDict([(b'f2', b'v2')])

    assert message3[0] == message_id3
    assert message3[1] == OrderedDict([(b'f3', b'v3')])


@pytest.mark.run_loop
@pytest.redis_version(999, 999, 999, reason="Streams only available on redis "
                                            "unstable branch")
async def test_xadd_maxlen_inexact(redis, server_bin):
    await redis.xadd('test_stream', {'f1': 'v1'})
    sleep(0.001)  # Ensure the millisecond-based message ID increments
    await redis.xadd('test_stream', {'f2': 'v2'})
    sleep(0.001)
    await redis.xadd('test_stream', {'f3': 'v3'}, max_len=2, exact_len=False)

    # Read it back
    messages = await redis.xrange('test_stream')
    # Redis will not have removed the whole node yet
    assert len(messages) == 3

    # Check the stream is eventually truncated
    for x in range(0, 1000):
        await redis.xadd('test_stream', {'f': 'v'}, max_len=2)

    messages = await redis.xrange('test_stream')
    assert len(messages) < 1000


@pytest.mark.run_loop
@pytest.redis_version(999, 999, 999, reason="Streams only available on redis "
                                            "unstable branch")
async def test_xrange(redis, server_bin):
    stream = 'test_stream'
    fields = OrderedDict((
        (b'field1', b'value1'),
        (b'field2', b'value2'),
    ))
    message_id1 = await redis.xadd(stream, fields)
    message_id2 = await redis.xadd(stream, fields)
    message_id3 = await redis.xadd(stream, fields)  # noqa

    # Test no parameters
    messages = await redis.xrange(stream)
    assert len(messages) == 3
    message = messages[0]
    assert message[0] == message_id1
    assert message[1] == OrderedDict([
        (b'field1', b'value1'),
        (b'field2', b'value2')]
    )

    # Test start
    messages = await redis.xrange(stream, start=message_id2)
    assert len(messages) == 2

    messages = await redis.xrange(stream, start='9900000000000-0')
    assert len(messages) == 0

    # Test stop
    messages = await redis.xrange(stream, stop='0000000000000-0')
    assert len(messages) == 0

    messages = await redis.xrange(stream, stop=message_id2)
    assert len(messages) == 2

    messages = await redis.xrange(stream, stop='9900000000000-0')
    assert len(messages) == 3

    # Test start & stop
    messages = await redis.xrange(stream,
                                  start=message_id1,
                                  stop=message_id2)
    assert len(messages) == 2

    messages = await redis.xrange(stream,
                                  start='0000000000000-0',
                                  stop='9900000000000-0')
    assert len(messages) == 3

    # Test count
    messages = await redis.xrange(stream, count=2)
    assert len(messages) == 2


@pytest.mark.run_loop
@pytest.redis_version(999, 999, 999, reason="Streams only available on redis "
                                            "unstable branch")
async def test_xrevrange(redis, server_bin):
    stream = 'test_stream'
    fields = OrderedDict((
        (b'field1', b'value1'),
        (b'field2', b'value2'),
    ))
    message_id1 = await redis.xadd(stream, fields)
    message_id2 = await redis.xadd(stream, fields)
    message_id3 = await redis.xadd(stream, fields)  # noqa

    # Test no parameters
    messages = await redis.xrevrange(stream)
    assert len(messages) == 3
    message = messages[0]
    assert message[0] == message_id3
    assert message[1] == OrderedDict([
        (b'field1', b'value1'),
        (b'field2', b'value2')]
    )

    # Test start
    messages = await redis.xrevrange(stream, start=message_id2)
    assert len(messages) == 2

    messages = await redis.xrevrange(stream, start='9900000000000-0')
    assert len(messages) == 3

    # Test stop
    messages = await redis.xrevrange(stream, stop='0000000000000-0')
    assert len(messages) == 3

    messages = await redis.xrevrange(stream, stop=message_id2)
    assert len(messages) == 2

    messages = await redis.xrevrange(stream, stop='9900000000000-0')
    assert len(messages) == 0

    # Test start & stop
    messages = await redis.xrevrange(stream,
                                     start=message_id2,
                                     stop=message_id1)
    assert len(messages) == 2

    messages = await redis.xrevrange(stream,
                                     start='9900000000000-0',
                                     stop='0000000000000-0')
    assert len(messages) == 3

    # Test count
    messages = await redis.xrevrange(stream, count=2)
    assert len(messages) == 2


@pytest.mark.run_loop
@pytest.redis_version(999, 999, 999, reason="Streams only available on redis "
                                            "unstable branch")
async def test_xread_selection(redis, server_bin):
    """Test use of counts and starting IDs"""
    stream = 'test_stream'
    fields = OrderedDict((
        (b'field1', b'value1'),
        (b'field2', b'value2'),
    ))
    message_id1 = await redis.xadd(stream, fields)
    message_id2 = await redis.xadd(stream, fields)  # noqa
    message_id3 = await redis.xadd(stream, fields)

    messages = await redis.xread([stream],
                                 timeout=1,
                                 latest_ids=['0000000000000-0'])
    assert len(messages) == 3

    messages = await redis.xread([stream],
                                 timeout=1,
                                 latest_ids=[message_id1])
    assert len(messages) == 2

    messages = await redis.xread([stream],
                                 timeout=1,
                                 latest_ids=[message_id3])
    assert len(messages) == 0

    messages = await redis.xread([stream],
                                 timeout=1,
                                 latest_ids=['0000000000000-0'], count=2)
    assert len(messages) == 2


@pytest.mark.run_loop
@pytest.redis_version(999, 999, 999, reason="Streams only available on redis "
                                            "unstable branch")
async def test_xread_blocking(redis, create_redis, loop, server, server_bin):
    """Test the blocking read features"""
    fields = OrderedDict((
        (b'field1', b'value1'),
        (b'field2', b'value2'),
    ))
    other_redis = await create_redis(
        server.tcp_address, loop=loop)

    # create blocking task in separate connection
    consumer = other_redis.xread(['test_stream'], timeout=1000)

    producer_task = asyncio.Task(
        add_message_with_sleep(redis, loop, 'test_stream', fields), loop=loop)
    results = await asyncio.gather(
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
    results = await redis.xread(['another_stream'], timeout=100)
    assert results == []

    other_redis.close()
