from collections import OrderedDict

from aioredis.util import wait_convert, wait_make_dict, wait_ok


def fields_to_dict(fields, type_=OrderedDict):
    """Convert a flat list of key/values into an OrderedDict"""
    fields_iterator = iter(fields)
    return type_(zip(fields_iterator, fields_iterator))


def parse_messages(messages):
    """ Parse messages as returned by Redis into something useful

    Messages returned by XRANGE arrive in the form:

        [
            [message_id, [key1, value1, key2, value2, ...]],
            ...
        ]

    Here we parse this into:

        [
            [message_id, OrderedDict(
                (key1, value1),
                (key2, value2),
                ...
            )],
            ...
        ]

    """
    if messages is None:
        return []
    return [(mid, fields_to_dict(values)) for mid, values in messages]


def parse_messages_by_stream(messages_by_stream):
    """ Parse messages returned by stream

    Messages returned by XREAD arrive in the form:
        [stream_name,
            [
                [message_id, [key1, value1, key2, value2, ...]],
                ...
            ],
            ...
        ]

    Here we parse this into (with the help of the above parse_messages()
    function):

        [
            [stream_name, message_id, OrderedDict(
                (key1, value1),
                (key2, value2),.
                ...
            )],
            ...
        ]

    """
    if messages_by_stream is None:
        return []

    parsed = []
    for stream, messages in messages_by_stream:
        for message_id, fields in parse_messages(messages):
            parsed.append((stream, message_id, fields))
    return parsed


def parse_lists_to_dicts(lists):
    """ Convert [[a, 1, b, 2], ...] into [{a:1, b: 2}, ...]"""
    return [fields_to_dict(l, type_=dict) for l in lists]


class StreamCommandsMixin:
    """Stream commands mixin

    Streams are under development in Redis and
    not currently released.
    """

    def xadd(self, stream, fields, message_id=b'*', max_len=None,
             exact_len=False):
        """Add a message to a stream."""
        args = []
        if max_len is not None:
            if exact_len:
                args.extend((b'MAXLEN', max_len))
            else:
                args.extend((b'MAXLEN', b'~', max_len))

        args.append(message_id)

        for k, v in fields.items():
            args.extend([k, v])
        return self.execute(b'XADD', stream, *args)

    def xrange(self, stream, start='-', stop='+', count=None):
        """Retrieve messages from a stream."""
        if count is not None:
            extra = ['COUNT', count]
        else:
            extra = []
        fut = self.execute(b'XRANGE', stream, start, stop, *extra)
        return wait_convert(fut, parse_messages)

    def xrevrange(self, stream, start='+', stop='-', count=None):
        """Retrieve messages from a stream in reverse order."""
        if count is not None:
            extra = ['COUNT', count]
        else:
            extra = []
        fut = self.execute(b'XREVRANGE', stream, start, stop, *extra)
        return wait_convert(fut, parse_messages)

    def xread(self, streams, timeout=0, count=None, latest_ids=None):
        """Perform a blocking read on the given stream

        :raises ValueError: if the length of streams and latest_ids do
                            not match
        """
        args = self._xread(streams, timeout, count, latest_ids)
        fut = self.execute(b'XREAD', *args)
        return wait_convert(fut, parse_messages_by_stream)

    def xread_group(self, group_name, consumer_name, streams, timeout=0,
                    count=None, latest_ids=None):
        """Perform a blocking read on the given stream as part of a consumer group

        :raises ValueError: if the length of streams and latest_ids do
                            not match
        """
        args = self._xread(streams, timeout, count, latest_ids)
        fut = self.execute(
            b'XREADGROUP', b'GROUP', group_name, consumer_name, *args
        )
        return wait_convert(fut, parse_messages_by_stream)

    def xgroup_create(self, stream, group_name, latest_id='$'):
        """Create a consumer group"""
        fut = self.execute(b'XGROUP', b'CREATE', stream, group_name, latest_id)
        return wait_ok(fut)

    def xgroup_setid(self, stream, group_name, latest_id='$'):
        """Set the latest ID for a consumer group"""
        fut = self.execute(b'XGROUP', b'SETID', stream, group_name, latest_id)
        return wait_ok(fut)

    def xgroup_destroy(self, stream, group_name):
        """Delete a consumer group"""
        fut = self.execute(b'XGROUP', b'DESTROY', stream, group_name)
        return wait_ok(fut)

    def xgroup_delconsumer(self, stream, group_name, consumer_name):
        """Delete a specific consumer from a group"""
        fut = self.execute(
            b'XGROUP', b'DELCONSUMER', stream, group_name, consumer_name
        )
        return wait_convert(fut, int)

    def xpending(self, stream, group_name, start=None, stop=None, count=None,
                 consumer=None):
        """Get information on pending messages for a stream

        Returned data will vary depending on the presence (or not)
        of the start/stop/count parameters. For more details see:
        https://redis.io/commands/xpending

        :raises ValueError: if the start/stop/count parameters are only
                            partially specified
        """
        # Returns: total pel messages, min id, max id, count
        ssc = [start, stop, count]
        ssc_count = len([v for v in ssc if v is not None])
        if ssc_count != 3 and ssc_count != 0:
            raise ValueError(
                'Either specify non or all of the start/stop/count arguments'
            )
        if not any(ssc):
            ssc = []

        args = [stream, group_name] + ssc
        if consumer:
            args.append(consumer)
        return self.execute(b'XPENDING', *args)

    def xclaim(self, stream, group_name, consumer_name, min_idle_time,
               id, *ids):
        """Claim a message for a given consumer"""
        fut = self.execute(
            b'XCLAIM', stream, group_name, consumer_name, min_idle_time,
            id, *ids
        )
        return wait_convert(fut, parse_messages)

    def xack(self, stream, group_name, id, *ids):
        """Acknowledge a message for a given consumer group"""
        return self.execute(b'XACK', stream, group_name, id, *ids)

    def xinfo(self, stream):
        """Retrieve information about the given stream.

        An alias for ``xinfo_stream()``
        """
        return self.xinfo_stream(stream)

    def xinfo_consumers(self, stream, group_name):
        """Retrieve consumers of a consumer group"""
        fut = self.execute(b'XINFO', b'CONSUMERS', stream, group_name)

        return wait_convert(fut, parse_lists_to_dicts)

    def xinfo_groups(self, stream):
        """Retrieve the consumer groups for a stream"""
        fut = self.execute(b'XINFO', b'GROUPS', stream)
        return wait_convert(fut, parse_lists_to_dicts)

    def xinfo_stream(self, stream):
        """Retrieve information about the given stream."""
        fut = self.execute(b'XINFO', b'STREAM', stream)
        return wait_make_dict(fut)

    def xinfo_help(self):
        """Retrieve help regarding the ``XINFO`` sub-commands"""
        fut = self.execute(b'XINFO', b'HELP')
        return wait_convert(fut, lambda l: b'\n'.join(l))

    def _xread(self, streams, timeout=0, count=None, latest_ids=None):
        """Wraps up common functionality between ``xread()``
        and ``xread_group()``

        You should probably be using ``xread()`` or ``xread_group()`` directly.
        """
        if latest_ids is None:
            latest_ids = ['$'] * len(streams)
        if len(streams) != len(latest_ids):
            raise ValueError(
                'The streams and latest_ids parameters must be of the '
                'same length'
            )

        count_args = [b'COUNT', count] if count else []
        if timeout is None:
            block_args = []
        else:
            block_args = [b'BLOCK', timeout]
        return block_args + count_args + [b'STREAMS'] + streams + latest_ids
