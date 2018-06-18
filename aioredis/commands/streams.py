from collections import OrderedDict

from aioredis.util import wait_convert, wait_make_dict, wait_ok


def fields_to_dict(fields, type_=OrderedDict):
    """Convert a flat list of key/values into an OrderedDict"""
    fields_iterator = iter(fields)
    return OrderedDict(zip(fields_iterator, fields_iterator))


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
        """ Add a message to the specified stream
        """
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
        """Retrieve stream data"""
        if count is not None:
            extra = ['COUNT', count]
        else:
            extra = []
        fut = self.execute(b'XRANGE', stream, start, stop, *extra)
        return wait_convert(fut, parse_messages)

    def xrevrange(self, stream, start='+', stop='-', count=None):
        """Retrieve stream data"""
        if count is not None:
            extra = ['COUNT', count]
        else:
            extra = []
        fut = self.execute(b'XREVRANGE', stream, start, stop, *extra)
        return wait_convert(fut, parse_messages)

    def xread(self, streams, timeout=0, count=None, latest_ids=None):
        """Perform a blocking read on the given stream"""
        args = self._xread(streams, timeout, count, latest_ids)
        fut = self.execute(b'XREAD', *args)
        return wait_convert(fut, parse_messages_by_stream)

    def xread_group(self, group_name, consumer_name, streams, timeout=0,
                    count=None, latest_ids=None):
        args = self._xread(streams, timeout, count, latest_ids)
        fut = self.execute(
            b'XREADGROUP', b'GROUP', group_name, consumer_name, *args
        )
        return wait_convert(fut, parse_messages_by_stream)

    def xgroup_create(self, stream, group_name, latest_id='$'):
        fut = self.execute(b'XGROUP', b'CREATE', stream, group_name, latest_id)
        return wait_ok(fut)

    def xgroup_setid(self, stream, group_name, latest_id='$'):
        fut = self.execute(b'XGROUP', b'SETID', stream, group_name, latest_id)
        return wait_ok(fut)

    def xgroup_delgroup(self, stream, group_name):
        fut = self.execute(b'XGROUP', b'DELGROUP', stream, group_name)
        return wait_ok(fut)

    def xgroup_delconsumer(self, stream, consumer_name):
        fut = self.execute(b'XGROUP', b'DELCONSUMER', stream, consumer_name)
        return wait_ok(fut)

    def xpending(self, stream, group_name, start=None, stop=None, count=None,
                 consumer=None):
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
        fut = self.execute(
            b'XCLAIM', stream, group_name, consumer_name, min_idle_time,
            id, *ids
        )
        return wait_convert(fut, parse_messages)

    def xack(self, stream, group_name, id, *ids):
        return self.execute(b'XACK', stream, group_name, id, *ids)

    def xinfo(self, stream):
        return self.xinfo_stream(stream)

    def xinfo_consumers(self, stream, group_name):
        fut = self.execute(b'XINFO', b'CONSUMERS', stream, group_name)

        return wait_convert(fut, parse_lists_to_dicts)

    def xinfo_groups(self, stream):
        fut = self.execute(b'XINFO', b'GROUPS', stream)
        return wait_convert(fut, parse_lists_to_dicts)

    def xinfo_stream(self, stream):
        fut = self.execute(b'XINFO', b'STREAM', stream)
        return wait_make_dict(fut)

    def xinfo_help(self):
        fut = self.execute(b'XINFO', b'HELP')
        return wait_convert(fut, lambda l: b'\n'.join(l))

    def _xread(self, streams, timeout=0, count=None, latest_ids=None):
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
