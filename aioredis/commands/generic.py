import asyncio


class GenericCommandsMixin:
    """Generic commands mixin.

    For commands details see: http://redis.io/commands/#generic
    """

    @asyncio.coroutine
    def delete(self, key, *keys):
        """Delete a key.
        """
        return
        yield

    @asyncio.coroutine
    def dump(self):
        pass

    @asyncio.coroutine
    def exists(self):
        pass

    @asyncio.coroutine
    def expire(self):
        pass

    @asyncio.coroutine
    def expireat(self):
        pass

    @asyncio.coroutine
    def keys(self):
        pass
