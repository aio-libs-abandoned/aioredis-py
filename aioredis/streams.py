import asyncio
from .log import logger
from .errors import ReplyError

class ReadGroupStream:
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
    def __init__(self, stream, check_pending, group_name, consumer_name, redis, count=10, encoding=None):
        self._queue = asyncio.Queue()
        self._stream = stream
        self._group_name = group_name
        self._consumer_name = consumer_name
        self._count = count
        self._redis = redis
        self._check_pending = check_pending
        self._encoding = encoding

    async def create_group(self):
        stream = self._stream[0]
        try:
            await self._redis.xgroup_create(stream=stream, group_name=self._group_name)
        except ReplyError as err:
            if "BUSYGROUP" in str(err):
                logger.info("Group `%s` for stream `%s` already exist" % (self._group_name, stream))
            else:
                raise

    async def ack_message(self, id):
        stream = self._stream[0]
        group_name = self._group_name
        await self._redis.xack(stream=stream, group_name=group_name, id=id)

    def __repr__(self):
        return "<{} name:{!r}, qsize:{}>".format(
            self.__class__.__name__,
            self._streams, self._queue.qsize())

    async def _get_messages(self):

        if self._check_pending:
            latest_ids = ['0']
        else:
            latest_ids = [">"]

        messages = await self._redis.xread_group(group_name=self._group_name, consumer_name=self._consumer_name,
        streams=self._stream, count=self._count, latest_ids=latest_ids, encoding=self._encoding)

        self._check_pending = False if len(messages) == 0 else True

        logger.info("Received %d messages..." % len(messages))
        for message in messages:
            await self._queue.put(message)

    def __repr__(self):
        return "<{} name:{!r}, qsize:{}>".format(
            self.__class__.__name__,
            self._streams, self._queue.qsize())

    async def get(self):
        """Coroutine that waits for and returns a message.

        :raises aioredis.ChannelClosedError: If channel is unsubscribed
            and has no messages.
        """
        while self._queue.empty():
            logger.debug("Empty queue....")
            await self._get_messages()
        msg = await self._queue.get()
        return msg


class ReadStream:
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
    def __init__(self, streams, latest_ids, redis, count=10, encoding=None):
        self._queue = asyncio.Queue()
        self._streams = streams
        self._count = count
        self._redis = redis
        self._latest_ids = latest_ids
        self._encoding = encoding
        self._memory = dict(zip(streams, latest_ids))

    def _stream_with_latest_ids(self):
        streams = []
        latest_ids = []
        for stream, latest_id in self._memory.items():
            streams.append(stream)
            latest_ids.append(latest_id)
        return streams, latest_ids

    async def _get_messages(self):
        streams, latest_ids = self._stream_with_latest_ids()
        messages = await self._redis.xread(streams=streams, count=self._count, latest_ids=latest_ids, encoding=self._encoding)
        logger.info("Received %d messages..." % len(messages))
        for message in messages:
            await self._queue.put(message)

    def __repr__(self):
        return "<{} name:{!r}, qsize:{}>".format(
            self.__class__.__name__,
            self._streams, self._queue.qsize())

    async def get(self):
        """Coroutine that waits for and returns a message.

        :raises aioredis.ChannelClosedError: If channel is unsubscribed
            and has no messages.
        """
        if self._queue.empty():
            logger.debug("Empty queue....")
            await self._get_messages()
        msg = await self._queue.get()
        self._memory[msg[0]] = msg[1]
        return msg
