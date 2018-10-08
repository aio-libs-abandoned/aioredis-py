import asyncio
from .log import logger
from .errors import ReplyError


class ReadStreams:
    """
    sub = await aioredis.create_redis(
     'redis://localhost')

    async def async_reader2(channel):
    while True:
        msg = await channel.get(encoding='utf-8')
        if msg is None:
            break
        # ... process message ...
        print("message in {}: {}".format(channel.name, msg))


    """
    def __init__(self, redis):
        self._redis = redis
        self._is_group = False
        self._memory = {}
        self._queue = asyncio.Queue()

    def __repr__(self):
        return "<{} name:{!r}, qsize:{}>".format(
            self.__class__.__name__,
            self._streams, self._queue.qsize())

    @property
    def is_group(self):
        return self._is_group

    @property
    def last_id_for_stream(self):
        return self._memory

    def consumer(self, streams, *, latest_ids=None, count=10, check_pending=True, group_name=None, consumer_name=None, encoding=None):
        if latest_ids is None:
            self._latest_ids = ["$"]

        self._count = count
        self._encoding = encoding
        self._streams = streams

        self._memory = dict(zip(self._streams, self._latest_ids))
        return self


    def consumer_as_group(self, streams, *, group_name, consumer_name, latest_ids=None, count=10, check_pending=True, encoding=None):
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

        return self

    async def ack_message(self, id):
        if self._is_group is False:
            raise ValueError("You didn't initialize it has a group")

        stream = self._streams[0]
        group_name = self._group_name
        await self._redis.xack(stream=stream, group_name=group_name, id=id)

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
        else:
            latest_ids = [">"]

        messages = await self._redis.xread_group(group_name=self._group_name, consumer_name=self._consumer_name,
        streams=self._streams, count=self._count, latest_ids=latest_ids, encoding=self._encoding)

        self._check_pending = False if len(messages) == 0 else True

        logger.info("Received %d messages..." % len(messages))
        for message in messages:
            await self._queue.put(message)

    async def _get_messages(self):
        streams, latest_ids = self._stream_with_latest_ids()
        messages = await self._redis.xread(streams=streams, count=self._count, latest_ids=latest_ids, encoding=self._encoding)
        logger.info("Received %d messages..." % len(messages))
        for message in messages:
            await self._queue.put(message)

    async def get(self):
        """Coroutine that waits for and returns a message.

        :raises aioredis.ChannelClosedError: If channel is unsubscribed
            and has no messages.
        """
        while self._queue.empty():
            logger.debug("Empty queue....")

            if self._is_group is True:
                await self._get_messages_from_group()
            else:
                await self._get_messages()

        msg = await self._queue.get()
        self._memory[msg[0]] = msg[1]

        return msg
