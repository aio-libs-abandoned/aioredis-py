import pytest
import asyncio

from collections import OrderedDict
from unittest import mock

from aioredis import ReplyError
from _testutils import redis_version

pytestmark = redis_version(
    5, 0, 0, reason="Streams only available since Redis 5.0.0")


@asyncio.coroutine
async def add_message_with_sleep(redis, loop, stream, fields):
    await asyncio.sleep(0.2, loop=loop)
    result = await redis.xadd(stream, fields)
    return result


@pytest.mark.run_loop
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
async def test_xadd_maxlen_exact(redis, server_bin):
    message_id1 = await redis.xadd('test_stream', {'f1': 'v1'})  # noqa

    # Ensure the millisecond-based message ID increments
    await asyncio.sleep(0.001)
    message_id2 = await redis.xadd('test_stream', {'f2': 'v2'})
    await asyncio.sleep(0.001)
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
async def test_xadd_manual_message_ids(redis, server_bin):
    await redis.xadd('test_stream', {'f1': 'v1'}, message_id='1515958771000-0')
    await redis.xadd('test_stream', {'f1': 'v1'}, message_id='1515958771000-1')
    await redis.xadd('test_stream', {'f1': 'v1'}, message_id='1515958772000-0')

    messages = await redis.xrange('test_stream')
    message_ids = [message_id for message_id, _ in messages]
    assert message_ids == [
        b'1515958771000-0',
        b'1515958771000-1',
        b'1515958772000-0'
    ]


@pytest.mark.run_loop
async def test_xadd_maxlen_inexact(redis, server_bin):
    await redis.xadd('test_stream', {'f1': 'v1'})
    # Ensure the millisecond-based message ID increments
    await asyncio.sleep(0.001)
    await redis.xadd('test_stream', {'f2': 'v2'})
    await asyncio.sleep(0.001)
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


@pytest.mark.run_loop
async def test_xgroup_create(redis, server_bin):
    # Also tests xinfo_groups()
    # TODO: Remove xadd() if resolved:
    #       https://github.com/antirez/redis/issues/4824
    await redis.xadd('test_stream', {'a': 1})
    await redis.xgroup_create('test_stream', 'test_group')
    info = await redis.xinfo_groups('test_stream')
    assert info == [{
        b'name': b'test_group',
        b'last-delivered-id': mock.ANY,
        b'pending': 0,
        b'consumers': 0
    }]


@pytest.mark.run_loop
async def test_xgroup_create_already_exists(redis, server_bin):
    await redis.xadd('test_stream', {'a': 1})
    await redis.xgroup_create('test_stream', 'test_group')
    with pytest.raises(ReplyError):
        await redis.xgroup_create('test_stream', 'test_group')


@pytest.mark.run_loop
async def test_xgroup_setid(redis, server_bin):
    await redis.xadd('test_stream', {'a': 1})
    await redis.xgroup_create('test_stream', 'test_group')
    await redis.xgroup_setid('test_stream', 'test_group', '$')


@pytest.mark.run_loop
async def test_xgroup_destroy(redis, server_bin):
    await redis.xadd('test_stream', {'a': 1})
    await redis.xgroup_create('test_stream', 'test_group')
    await redis.xgroup_destroy('test_stream', 'test_group')
    info = await redis.xinfo_groups('test_stream')
    assert not info


@pytest.mark.run_loop
async def test_xread_group(redis):
    await redis.xadd('test_stream', {'a': 1})
    await redis.xgroup_create('test_stream', 'test_group', latest_id='0')

    messages = await redis.xread_group(
        'test_group', 'test_consumer', ['test_stream'],
        timeout=1000, latest_ids=[0]
    )
    assert len(messages) == 1
    stream, message_id, fields = messages[0]
    assert stream == b'test_stream'
    assert message_id
    assert fields == {b'a': b'1'}


@pytest.mark.run_loop
async def test_xack_and_xpending(redis):
    # Test a full xread -> xack cycle, using xpending to check the status
    message_id = await redis.xadd('test_stream', {'a': 1})
    await redis.xgroup_create('test_stream', 'test_group', latest_id='0')

    # Nothing pending as we haven't claimed anything yet
    pending_count, min_id, max_id, count = \
        await redis.xpending('test_stream', 'test_group')
    assert pending_count == 0

    # Read the message
    await redis.xread_group(
        'test_group', 'test_consumer', ['test_stream'],
        timeout=1000, latest_ids=[0]
    )

    # It is now pending
    pending_count, min_id, max_id, pel = \
        await redis.xpending('test_stream', 'test_group')
    assert pending_count == 1
    assert min_id == message_id
    assert max_id == message_id
    assert pel == [[b'test_consumer', b'1']]

    # Acknowledge the message
    await redis.xack('test_stream', 'test_group', message_id)

    # It is no longer pending
    pending_count, min_id, max_id, pel = \
        await redis.xpending('test_stream', 'test_group')
    assert pending_count == 0


@pytest.mark.run_loop
async def test_xpending_get_messages(redis):
    # Like test_xack_and_xpending(), but using the start/end xpending()
    # params to get the messages
    message_id = await redis.xadd('test_stream', {'a': 1})
    await redis.xgroup_create('test_stream', 'test_group', latest_id='0')
    await redis.xread_group(
        'test_group', 'test_consumer', ['test_stream'],
        timeout=1000, latest_ids=[0]
    )
    await asyncio.sleep(0.05)

    # It is now pending
    response = await redis.xpending('test_stream', 'test_group', '-', '+', 10)
    assert len(response) == 1
    (
        message_id, consumer_name,
        milliseconds_since_last_delivery, num_deliveries
    ) = response[0]

    assert message_id
    assert consumer_name == b'test_consumer'
    assert milliseconds_since_last_delivery >= 50
    assert num_deliveries == 1


@pytest.mark.run_loop
async def test_xpending_start_of_zero(redis):
    await redis.xadd('test_stream', {'a': 1})
    await redis.xgroup_create('test_stream', 'test_group', latest_id='0')
    # Doesn't raise a value error
    await redis.xpending('test_stream', 'test_group', 0, '+', 10)


@pytest.mark.run_loop
async def test_xclaim_simple(redis):
    # Put a message in a pending state then reclaim it is XCLAIM
    message_id = await redis.xadd('test_stream', {'a': 1})
    await redis.xgroup_create('test_stream', 'test_group', latest_id='0')
    await redis.xread_group(
        'test_group', 'test_consumer', ['test_stream'],
        timeout=1000, latest_ids=[0]
    )

    # Message is now pending
    pending_count, min_id, max_id, pel = \
        await redis.xpending('test_stream', 'test_group')
    assert pending_count == 1
    assert pel == [[b'test_consumer', b'1']]

    # Now claim it for another consumer
    result = await redis.xclaim('test_stream', 'test_group', 'new_consumer',
                                min_idle_time=0, id=message_id)
    assert result
    claimed_message_id, fields = result[0]
    assert claimed_message_id == message_id
    assert fields == {b'a': b'1'}

    # Ok, no see how things look
    pending_count, min_id, max_id, pel = \
        await redis.xpending('test_stream', 'test_group')
    assert pending_count == 1
    assert pel == [[b'new_consumer', b'1']]


@pytest.mark.run_loop
async def test_xclaim_min_idle_time_includes_messages(redis):
    message_id = await redis.xadd('test_stream', {'a': 1})
    await redis.xgroup_create('test_stream', 'test_group', latest_id='0')
    await redis.xread_group(
        'test_group', 'test_consumer', ['test_stream'],
        timeout=1000, latest_ids=[0]
    )

    # Message is now pending. Wait 100ms
    await asyncio.sleep(0.1)

    # Now reclaim any messages which have been idle for > 50ms
    result = await redis.xclaim('test_stream', 'test_group', 'new_consumer',
                                min_idle_time=50, id=message_id)
    assert result


@pytest.mark.run_loop
async def test_xclaim_min_idle_time_excludes_messages(redis):
    message_id = await redis.xadd('test_stream', {'a': 1})
    await redis.xgroup_create('test_stream', 'test_group', latest_id='0')
    await redis.xread_group(
        'test_group', 'test_consumer', ['test_stream'],
        timeout=1000, latest_ids=[0]
    )
    # Message is now pending. Wait no time at all

    # Now reclaim any messages which have been idle for > 50ms
    result = await redis.xclaim('test_stream', 'test_group', 'new_consumer',
                                min_idle_time=50, id=message_id)
    # Nothing to claim
    assert not result


@pytest.mark.run_loop
async def test_xgroup_delconsumer(redis, create_redis, server):
    await redis.xadd('test_stream', {'a': 1})
    await redis.xgroup_create('test_stream', 'test_group')

    # Note that consumers are only created once they read a message,
    # not when they first connect. So make sure we consume from ID 0
    # so we get the messages we just XADDed (above)
    await redis.xread_group(
        'test_group', 'test_consumer',
        streams=['test_stream'], latest_ids=[0]
    )

    response = await redis.xgroup_delconsumer(
        'test_stream', 'test_group', 'test_consumer'
    )
    assert response == 0
    info = await redis.xinfo_consumers('test_stream', 'test_group')
    assert not info


@pytest.mark.run_loop
async def test_xinfo_consumers(redis):
    await redis.xadd('test_stream', {'a': 1})
    await redis.xgroup_create('test_stream', 'test_group')

    # Note that consumers are only created once they read a message,
    # not when they first connect. So make sure we consume from ID 0
    # so we get the messages we just XADDed (above)
    await redis.xread_group(
        'test_group', 'test_consumer',
        streams=['test_stream'], latest_ids=[0]
    )

    info = await redis.xinfo_consumers('test_stream', 'test_group')
    assert info
    assert isinstance(info[0], dict)


@pytest.mark.run_loop
async def test_xinfo_stream(redis):
    await redis.xadd('test_stream', {'a': 1})
    await redis.xgroup_create('test_stream', 'test_group')

    # Note that consumers are only created once they read a message,
    # not when they first connect. So make sure we consume from ID 0
    # so we get the messages we just XADDed (above)
    await redis.xread_group(
        'test_group', 'test_consumer',
        streams=['test_stream'], latest_ids=[0]
    )

    info = await redis.xinfo_stream('test_stream')
    assert info
    assert isinstance(info, dict)

    info = await redis.xinfo('test_stream')
    assert info
    assert isinstance(info, dict)


@pytest.mark.run_loop
async def test_xinfo_help(redis):
    info = await redis.xinfo_help()
    assert info
