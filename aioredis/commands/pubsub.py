import json


class PubSubCommandsMixin:
    """Pub/Sub commands mixin.

    For commands details see: http://redis.io/commands/#pubsub
    """

    def publish(self, channel, message):
        """Post a message to channel."""
        return self._conn.execute(b'PUBLISH', channel, message)

    def publish_json(self, channel, obj):
        """Post a JSON-encoded message to channel."""
        return self.publish(channel, json.dumps(obj))

    def subscribe(self, channel, *channels):
        """Switch connection to Pub/Sub mode and
        subscribe to specified channels.
        """
        # TODO: Implement
        pass

    def unsubscribe(self, channel, *channels):
        pass

    def psubscribe(self, pattern, *patterns):
        """Switch connection to Pub/Sub mode and
        subscribe to specified patterns.
        """
        # TODO: Implement
        pass

    def punsubscribe(self, pattern, *patterns):
        pass

    def pubsub_channels(self, pattern=None):
        """Lists the currently active channels."""
        args = [b'PUBSUB', b'CHANNELS']
        if pattern is not None:
            args.append(pattern)
        return self._conn.execute(*args)

    def pubsub_numsub(self, *channels):
        """Returns the number of subscribers for the specified channels."""
        return self._conn.execute(b'PUBSUB', b'NUMSUB', *channels)

    def pubsub_numpat(self):
        """Returns the number of subscriptions to patterns."""
        return self._conn.execute(b'PUBSUB', b'NUMPAT')
