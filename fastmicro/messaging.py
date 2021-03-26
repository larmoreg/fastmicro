import abc
import asyncio
from contextlib import asynccontextmanager
import logging
import os
from typing import (
    Any,
    AsyncIterator,
    Dict,
    Generic,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

import aioredis
import pulsar
import pydantic

from .topic import Header, Topic

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=pydantic.BaseModel)


class Messaging(abc.ABC):
    async def connect(self) -> None:
        pass

    async def cleanup(self) -> None:
        pass

    async def _prepare(self, topic_name: str, group_name: str) -> None:
        pass

    @abc.abstractmethod
    async def _receive(
        self, topic_name: str, group_name: str, user_name: str
    ) -> Tuple[bytes, bytes]:
        raise NotImplementedError

    @abc.abstractmethod
    async def _ack(self, topic_name: str, group_name: str, message_id: bytes) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def _nack(self, topic_name: str, group_name: str, message_id: bytes) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def _send(self, topic_name: str, serialized: bytes) -> None:
        raise NotImplementedError

    @asynccontextmanager
    async def receive(
        self, topic: Topic[T], group_name: str, consumer_name: str
    ) -> AsyncIterator[Header]:
        try:
            await self._prepare(topic.name, group_name)
            message_id, serialized = await self._receive(topic.name, group_name, consumer_name)
            logger.debug(f"Received {message_id!r} {serialized!r}")
            header = await topic.deserialize(message_id, serialized)
            yield header
            assert header.id
            logger.debug(f"Acking {header.id!r}")
            await self._ack(topic.name, group_name, header.id)
        except Exception:
            logger.exception("Processing failed")
            assert header.id
            logger.debug(f"Nacking {header.id!r}")
            await self._nack(topic.name, group_name, header.id)

    async def send(self, topic: Topic[T], message: Union[Header, Any]) -> Header:
        if isinstance(message, Header):
            header = message
        else:
            header = Header(message=message)
        serialized = await topic.serialize(header)
        logger.debug(f"Sending {serialized!r}")
        await self._send(topic.name, serialized)
        return header


QT = TypeVar("QT")


class Queue(Generic[QT]):
    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        self.lock: asyncio.Lock = asyncio.Lock(loop=loop)
        self.queue: asyncio.Queue[Tuple[bytes, QT]] = asyncio.Queue(loop=loop)
        self.index: int = 0
        self.items: Dict[bytes, QT] = dict()
        self.pending: Set[bytes] = set()
        self.nacked: List[bytes] = list()

    async def get(self) -> Tuple[bytes, QT]:
        async with self.lock:
            if self.nacked:
                message_id = self.nacked.pop()
            else:
                message_id, item = await self.queue.get()
                self.items[message_id] = item
            self.pending.add(message_id)
        return message_id, self.items[message_id]

    async def put(self, item: QT) -> bytes:
        message_id = str(self.index).encode()
        self.index += 1
        await self.queue.put((message_id, item))
        return message_id

    async def ack(self, message_id: bytes) -> None:
        async with self.lock:
            if message_id in self.pending:
                self.pending.remove(message_id)

    async def nack(self, message_id: bytes) -> None:
        async with self.lock:
            if message_id in self.pending:
                self.pending.remove(message_id)
                self.nacked.insert(0, message_id)


class MemoryMessaging(Messaging):
    def __init__(
        self,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        self.loop = loop
        self.lock: asyncio.Lock = asyncio.Lock(loop=self.loop)
        self.queues: Dict[str, Queue[bytes]] = dict()

    async def _get_queue(self, topic_name: str) -> Queue[bytes]:
        async with self.lock:
            if topic_name not in self.queues:
                self.queues[topic_name] = Queue(self.loop)
            return self.queues[topic_name]

    async def _receive(
        self, topic_name: str, group_name: str, user_name: str
    ) -> Tuple[bytes, bytes]:
        queue = await self._get_queue(topic_name)
        message_id, serialized = await queue.get()
        return message_id, serialized

    async def _ack(self, topic_name: str, group_name: str, message_id: bytes) -> None:
        queue = await self._get_queue(topic_name)
        await queue.ack(message_id)

    async def _nack(self, topic_name: str, group_name: str, message_id: bytes) -> None:
        queue = await self._get_queue(topic_name)
        await queue.nack(message_id)

    async def _send(self, topic_name: str, serialized: bytes) -> None:
        queue = await self._get_queue(topic_name)
        await queue.put(serialized)


class RedisMessaging(Messaging):  # pragma: no cover
    def __init__(
        self,
        address: str = "redis://localhost:6379",
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        self.address = os.getenv("ADDRESS", address)
        if loop:
            self.loop = loop
        else:
            self.loop = asyncio.get_event_loop()
        self.pool = None

    async def connect(self) -> None:
        self.pool = await aioredis.create_pool(self.address, loop=self.loop)

    async def cleanup(self) -> None:
        assert self.pool
        self.pool.close()
        await self.pool.wait_closed()

    async def _topic_exists(self, topic_name: str) -> bool:
        try:
            assert self.pool
            with await self.pool as connection:
                redis = aioredis.Redis(connection)
                await redis.xinfo_stream(topic_name)
        except aioredis.errors.ReplyError:
            return False
        return True

    async def _group_exists(self, topic_name: str, group_name: str) -> bool:
        assert self.pool
        with await self.pool as connection:
            redis = aioredis.Redis(connection)
            groups_info = await redis.xinfo_groups(topic_name)
            for group_info in groups_info:
                if group_info[b"name"] == group_name.encode():
                    return True
            return False

    async def _create_group(self, topic_name: str, group_name: str) -> None:
        if not await self._topic_exists(topic_name) or not await self._group_exists(
            topic_name, group_name
        ):
            assert self.pool
            with await self.pool as connection:
                redis = aioredis.Redis(connection)
                await redis.xgroup_create(topic_name, group_name, latest_id="$", mkstream=True)

    async def _prepare(self, topic_name: str, group_name: str) -> None:
        await self._create_group(topic_name, group_name)

    async def _receive(
        self, topic_name: str, group_name: str, user_name: str
    ) -> Tuple[bytes, bytes]:
        assert self.pool
        with await self.pool as connection:
            redis = aioredis.Redis(connection)
            messages = await redis.xread_group(
                group_name, user_name, [topic_name], latest_ids=[">"]
            )
            stream, message_id, message = messages[-1]
            return message_id, message[b"data"]

    async def _ack(self, topic_name: str, group_name: str, id: bytes) -> None:
        assert self.pool
        with await self.pool as connection:
            redis = aioredis.Redis(connection)
            await redis.xack(topic_name, group_name, id)

    async def _nack(self, topic_name: str, group_name: str, id: bytes) -> None:
        pass

    async def _send(self, topic_name: str, serialized: bytes) -> None:
        assert self.pool
        with await self.pool as connection:
            redis = aioredis.Redis(connection)
            await redis.xadd(topic_name, {"data": serialized})


class PulsarMessaging(Messaging):  # pragma: no cover
    def __init__(
        self,
        service_url: str = "pulsar://localhost:6650",
    ) -> None:
        service_url = os.getenv("SERVICE_URL", service_url)
        self.client = pulsar.Client(service_url)
        self.consumers: Dict[Tuple[str, str], pulsar.Consumer] = dict()
        self.producers: Dict[str, pulsar.Producer] = dict()

    async def cleanup(self) -> None:
        self.client.close()

    async def _get_consumer(self, topic_name: str, group_name: str) -> pulsar.Consumer:
        key = topic_name, group_name
        return self.consumers[key]

    async def _get_producer(self, topic_name: str) -> pulsar.Producer:
        if topic_name not in self.producers:
            self.producers[topic_name] = self.client.create_producer(topic_name)
        return self.producers[topic_name]

    async def _prepare(self, topic_name: str, group_name: str) -> None:
        key = topic_name, group_name
        if key not in self.consumers:
            self.consumers[key] = self.client.subscribe(topic_name, group_name)

    async def _receive(
        self, topic_name: str, group_name: str, user_name: str
    ) -> Tuple[bytes, bytes]:
        consumer = await self._get_consumer(topic_name, group_name)
        message = consumer.receive()
        return message.message_id(), message.data()

    async def _ack(self, topic_name: str, group_name: str, id: bytes) -> None:
        temp = pulsar.MessageId.deserialize(id)
        consumer = await self._get_consumer(topic_name, group_name)
        consumer.acknowledge(temp)

    async def _nack(self, topic_name: str, group_name: str, id: bytes) -> None:
        temp = pulsar.MessageId.deserialize(id)
        consumer = await self._get_consumer(topic_name, group_name)
        consumer.negative_acknowledge(temp)

    async def _send(self, topic_name: str, serialized: bytes) -> None:
        producer = await self._get_producer(topic_name)
        producer.send(serialized)
