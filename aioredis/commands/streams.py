import sys
from collections import OrderedDict

from aioredis.util import wait_convert

PY_VER = sys.version_info

if PY_VER < (3, 0):
    from itertools import izip as zip


def fields_to_dict(fields):
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


class StreamCommandsMixin:
    """Stream commands mixin

    Streams are under development in Redis and
    not currently released.
    """

    def xadd(self, stream, fields=None):
        """ Add a message to the specified stream
        """
        # TODO: Add the MAXLEN parameter
        flattened = []
        for k, v in fields.items():
            flattened += [k, v]
        return self.execute(b'XADD', stream, '*', *flattened)

    def xrange(self, stream, start='-', stop='+', count=None):
        """Retrieve stream data"""
        if count is not None:
            extra = ['COUNT', count]
        else:
            extra = []
        fut = self.execute(b'XRANGE', stream, start, stop, *extra)
        return wait_convert(fut, parse_messages)

    def xread(self, streams, timeout=0, count=None, latest_ids=None):
        """Perform a blocking read on the given stream"""
        # QUESTION: Should we combine streams & starting_ids into a single parameter?
        if latest_ids is None:
            latest_ids = ['$'] * len(streams)
        if len(streams) != len(latest_ids):
            raise ValueError('The streams and latest_ids parameters must be of the same length')

        count_args = [b'COUNT', count] if count else []
        args = count_args + [b'STREAMS'] + streams + latest_ids
        print(args)
        fut = self.execute(b'XREAD', b'BLOCK', timeout, *args)
        return wait_convert(fut, parse_messages_by_stream)
