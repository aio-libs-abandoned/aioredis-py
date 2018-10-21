"""
Redis Stream utils
"""

import asyncio
from types import MappingProxyType

from .log import logger


class ReadStreams:
    """
    Redis Streams pretty interface

    """
    def __init__(self, redis):
        self._redis = redis
        self._is_group = False
        self._memory = {}
        self._queue = asyncio.Queue()
        self._configured = False

    def __repr__(self):
        return "<{} name:{!r}, qsize:{}>".format(
            self.__class__.__name__,
            self._streams,
            self._queue.qsize()
        )

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._configured is False:
            raise ValueError("Streams have to be initialized correcly with `consumer` or `consumer_with_group` before")
        msg = await self.get()
        if msg:
            return msg
        else:
            raise StopAsyncIteration

    @property
    def is_group(self):
        return self._is_group

    @property
    def last_ids_for_stream(self):
        return MappingProxyType(self._memory)

    def consumer(
        self,
        streams,
        *,
        latest_ids=None,
        count=10,
        check_pending=True,
        group_name=None,
        consumer_name=None,
        encoding=None
    ):
        if latest_ids is None:
            self._latest_ids = ["$"]
        else:
            self._latest_ids = latest_ids

        self._count = count
        self._encoding = encoding
        self._streams = streams
        self._memory = dict(zip(self._streams, self._latest_ids))
        self._configured = True
        return self

    def consumer_with_group(
        self,
        streams,
        *,
        group_name,
        consumer_name,
        latest_ids=None,
        count=10,
        check_pending=True,
        encoding=None
    ):
        if len(streams) > 1:
            raise ValueError("We support only 1 stream when using group reads")

        if latest_ids is None:
            self._latest_ids = ["$"]

        self._is_group = True
        self._group_name = group_name
        self._consumer_name = consumer_name
        self._check_pending = check_pending
        self._count = count
        self._encoding = encoding
        self._streams = streams
        self._memory = dict(zip(self._streams, self._latest_ids))
        self._configured = True
        return self

    async def ack_message(self, id):
        if self._is_group is False:
            raise ValueError("You didn't initialize this stream as a group")

        stream = self._streams[0]
        group_name = self._group_name
        await self._redis.xack(stream=stream, group_name=group_name, id=id)
        logger.debug("<message:%s, stream:%s group_name:%s> acknowleged" % (id, stream, group_name))

    def _stream_with_latest_ids(self):
        streams = []
        latest_ids = []

        for stream, latest_id in self._memory.items():
            streams.append(stream)
            latest_ids.append(latest_id)

        return streams, latest_ids

    async def _get_messages_from_group(self):
        if self._check_pending:
            latest_ids = ['0']
            logger.debug("Checking pending messages for stream `%s`" % self._streams[0])
        else:
            latest_ids = [">"]

        messages = await self._redis.xread_group(
            group_name=self._group_name,
            consumer_name=self._consumer_name,
            streams=self._streams,
            count=self._count,
            latest_ids=latest_ids,
            encoding=self._encoding
        )

        self._check_pending = False if len(messages) == 0 else True

        logger.info("Received %d messages..." % len(messages))
        for message in messages:
            await self._queue.put(message)

    async def _get_messages(self):
        streams, latest_ids = self._stream_with_latest_ids()

        messages = await self._redis.xread(
            streams=streams,
            count=self._count,
            latest_ids=latest_ids,
            encoding=self._encoding
        )

        logger.info("Received %d messages..." % len(messages))

        for message in messages:
            await self._queue.put(message)

    async def get(self):
        """Coroutine that waits for and returns a message.

        :raises aioredis.ChannelClosedError: If channel is unsubscribed
            and has no messages.
        """
        while self._queue.empty():
            logger.debug("Empty queue, waiting for messages....")

            if self._is_group is True:
                await self._get_messages_from_group()
            else:
                await self._get_messages()

        msg = await self._queue.get()
        self._memory[msg[0]] = msg[1]

        return msg
