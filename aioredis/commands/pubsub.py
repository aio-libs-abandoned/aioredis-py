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
