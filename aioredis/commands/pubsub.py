import json

from aioredis.util import wait_make_dict


class PubSubCommandsMixin:
    """Pub/Sub commands mixin.

    For commands details see: http://redis.io/commands/#pubsub
    """

    def publish(self, channel, message):
        """Post a message to channel."""
        return self.execute(b'PUBLISH', channel, message)

    def publish_json(self, channel, obj):
        """Post a JSON-encoded message to channel."""
        return self.publish(channel, json.dumps(obj))

    def subscribe(self, channel, *channels):
        """Switch connection to Pub/Sub mode and
        subscribe to specified channels.

        Arguments can be instances of :class:`~aioredis.Channel`.

        Returns :func:`asyncio.gather()` coroutine which when done will return
        a list of :class:`~aioredis.Channel` objects.
        """
        conn = self._pool_or_conn
        return wait_return_channels(
            conn.execute_pubsub(b'SUBSCRIBE', channel, *channels),
            conn, 'pubsub_channels')

    def unsubscribe(self, channel, *channels):
        """Unsubscribe from specific channels.

        Arguments can be instances of :class:`~aioredis.Channel`.
        """
        conn = self._pool_or_conn
        return conn.execute_pubsub(b'UNSUBSCRIBE', channel, *channels)

    def psubscribe(self, pattern, *patterns):
        """Switch connection to Pub/Sub mode and
        subscribe to specified patterns.

        Arguments can be instances of :class:`~aioredis.Channel`.

        Returns :func:`asyncio.gather()` coroutine which when done will return
        a list of subscribed :class:`~aioredis.Channel` objects with
        ``is_pattern`` property set to ``True``.
        """
        conn = self._pool_or_conn
        return wait_return_channels(
            conn.execute_pubsub(b'PSUBSCRIBE', pattern, *patterns),
            conn, 'pubsub_patterns')

    def punsubscribe(self, pattern, *patterns):
        """Unsubscribe from specific patterns.

        Arguments can be instances of :class:`~aioredis.Channel`.
        """
        conn = self._pool_or_conn
        return conn.execute_pubsub(b'PUNSUBSCRIBE', pattern, *patterns)

    def pubsub_channels(self, pattern=None):
        """Lists the currently active channels."""
        args = [b'PUBSUB', b'CHANNELS']
        if pattern is not None:
            args.append(pattern)
        return self.execute(*args)

    def pubsub_numsub(self, *channels):
        """Returns the number of subscribers for the specified channels."""
        return wait_make_dict(self.execute(
            b'PUBSUB', b'NUMSUB', *channels))

    def pubsub_numpat(self):
        """Returns the number of subscriptions to patterns."""
        return self.execute(b'PUBSUB', b'NUMPAT')

    @property
    def channels(self):
        """Returns read-only channels dict.

        See :attr:`~aioredis.RedisConnection.pubsub_channels`
        """
        return self._pool_or_conn.pubsub_channels

    @property
    def patterns(self):
        """Returns read-only patterns dict.

        See :attr:`~aioredis.RedisConnection.pubsub_patterns`
        """
        return self._pool_or_conn.pubsub_patterns

    @property
    def in_pubsub(self):
        """Indicates that connection is in PUB/SUB mode.

        Provides the number of subscribed channels.
        """
        return self._pool_or_conn.in_pubsub


async def wait_return_channels(fut, conn, field):
    res = await fut
    channels_dict = getattr(conn, field)
    return [channels_dict[name] for cmd, name, count in res]
