import abc
import asyncio
import atexit
import os
from typing import Any, Dict, Generic, List, Optional, Set, Tuple, TypeVar
from uuid import UUID, uuid4

import aioredis
import pulsar
import pydantic

from .registrar import registrar


class Header(pydantic.BaseModel):
    id: Optional[bytes]
    data: Optional[bytes]

    uuid: Optional[UUID]
    parent: Optional[UUID]
    message: Optional[Any]


class Messaging(abc.ABC):
    def __init__(
        self,
        serializer: str = "msgpack",
    ) -> None:
        self.serializer = registrar.get(serializer)
        atexit.register(self.cleanup)

    def cleanup(self) -> None:
        pass

    async def serialize(self, header: Header) -> bytes:
        return await self.serializer.serialize(header.dict(exclude={"id", "message"}))

    async def deserialize(self, serialized: bytes) -> Header:
        header = await self.serializer.deserialize(serialized)
        return Header(**header)

    @abc.abstractmethod
    async def receive(self, topic_name: str, group_name: str, user_name: str) -> Header:
        raise NotImplementedError

    @abc.abstractmethod
    async def ack(self, topic_name: str, group_name: str, message_id: bytes) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def nack(self, topic_name: str, group_name: str, message_id: bytes) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def send(self, topic_name: str, header: Header) -> None:
        raise NotImplementedError


T = TypeVar("T")


class Queue(Generic[T]):
    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        self.lock: asyncio.Lock = asyncio.Lock(loop=loop)
        self.queue: asyncio.Queue[Tuple[bytes, T]] = asyncio.Queue(loop=loop)
        self.index: int = 0
        self.items: Dict[bytes, T] = dict()
        self.pending: Set[bytes] = set()
        self.nacked: List[bytes] = list()

    async def get(self) -> Tuple[bytes, T]:
        async with self.lock:
            if self.nacked:
                message_id = self.nacked.pop()
            else:
                message_id, item = await self.queue.get()
                self.items[message_id] = item
            self.pending.add(message_id)
        return message_id, self.items[message_id]

    async def put(self, item: T) -> bytes:
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
        serializer: str = "msgpack",
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        super().__init__(serializer)
        self.loop = loop
        self.lock: asyncio.Lock = asyncio.Lock(loop=self.loop)
        self.queues: Dict[str, Queue[bytes]] = dict()

    async def _get_queue(self, topic_name: str) -> Queue[bytes]:
        async with self.lock:
            if topic_name not in self.queues:
                self.queues[topic_name] = Queue(self.loop)
            return self.queues[topic_name]

    async def receive(self, topic_name: str, group_name: str, user_name: str) -> Header:
        queue = await self._get_queue(topic_name)
        message_id, serialized = await queue.get()
        header = await self.deserialize(serialized)
        header.id = message_id
        return header

    async def ack(self, topic_name: str, group_name: str, message_id: bytes) -> None:
        queue = await self._get_queue(topic_name)
        await queue.ack(message_id)

    async def nack(self, topic_name: str, group_name: str, message_id: bytes) -> None:
        queue = await self._get_queue(topic_name)
        await queue.nack(message_id)

    async def send(self, topic_name: str, header: Header) -> None:
        queue = await self._get_queue(topic_name)
        header.uuid = uuid4()
        serialized = await self.serialize(header)
        header.id = await queue.put(serialized)


class RedisMessaging(Messaging):  # pragma: no cover
    def __init__(
        self, serializer: str = "msgpack", address: str = "redis://localhost:6379", *args, **kwargs
    ) -> None:
        super().__init__(serializer)
        self.address = os.getenv("ADDRESS", address)

    @classmethod
    async def create(cls, *args, **kwargs):
        self = RedisMessaging(*args, **kwargs)
        self.redis = await aioredis.create_redis_pool(self.address)
        return self

    def cleanup(self) -> None:
        self.redis.close()

    async def _topic_exists(self, topic_name):
        try:
            await self.redis.xinfo_stream(topic_name)
        except aioredis.errors.ReplyError:
            return False
        return True

    async def _group_exists(self, topic_name, group_name):
        groups_info = await self.redis.xinfo_groups(topic_name)
        for group_info in groups_info:
            if group_info[b"name"] == group_name.encode():
                return True
        return False

    async def _create_group(self, topic_name, group_name):
        if not await self._topic_exists(topic_name) or not await self._group_exists(
            topic_name, group_name
        ):
            await self.redis.xgroup_create(topic_name, group_name, latest_id="$", mkstream=True)

    async def receive(self, topic_name: str, group_name: str, user_name: str) -> Header:
        await self._create_group(topic_name, group_name)
        messages = await self.redis.xread_group(
            group_name, user_name, [topic_name], latest_ids=[">"]
        )
        assert len(messages) == 1
        stream, message_id, message = messages[0]
        header = await self.deserialize(message[b"data"])
        header.id = message_id
        return header

    async def ack(self, topic_name: str, group_name: str, id: bytes) -> None:
        await self.redis.xack(topic_name, group_name, id)

    async def nack(self, topic_name: str, group_name: str, id: bytes) -> None:
        pass

    async def send(self, topic_name: str, header: Header) -> None:
        header.uuid = uuid4()
        serialized = await self.serialize(header)
        await self.redis.xadd(topic_name, {"data": serialized})


class PulsarMessaging(Messaging):  # pragma: no cover
    def __init__(
        self,
        serializer: str = "msgpack",
        service_url: str = "pulsar://localhost:6650",
        *args,
        **kwargs
    ) -> None:
        super().__init__(serializer)
        service_url = os.getenv("SERVICE_URL", service_url)
        self.client = pulsar.Client(service_url, *args, **kwargs)
        self.consumers: Dict[Tuple[str, str], pulsar.Consumer] = dict()
        self.producers: Dict[str, pulsar.Producer] = dict()

    def cleanup(self) -> None:
        self.client.close()

    async def _get_consumer(self, topic_name: str, group_name: str) -> pulsar.Consumer:
        key = topic_name, group_name
        if key not in self.consumers:
            self.consumers[key] = self.client.subscribe(topic_name, group_name)
        return self.consumers[key]

    async def _get_producer(self, topic_name: str) -> pulsar.Producer:
        if topic_name not in self.producers:
            self.producers[topic_name] = self.client.create_producer(topic_name)
        return self.producers[topic_name]

    async def receive(self, topic_name: str, group_name: str, user_name: str) -> Header:
        consumer = await self._get_consumer(topic_name, group_name)
        message = consumer.receive()
        header = await self.deserialize(message.data())
        header.id = message.message_id().serialize()
        return header

    async def ack(self, topic_name: str, group_name: str, id: bytes) -> None:
        temp = pulsar.MessageId.deserialize(id)
        consumer = await self._get_consumer(topic_name, group_name)
        consumer.acknowledge(temp)

    async def nack(self, topic_name: str, group_name: str, id: bytes) -> None:
        temp = pulsar.MessageId.deserialize(id)
        consumer = await self._get_consumer(topic_name, group_name)
        consumer.negative_acknowledge(temp)

    async def send(self, topic_name: str, header: Header) -> None:
        producer = await self._get_producer(topic_name)
        header.uuid = uuid4()
        serialized = await self.serialize(header)
        producer.send(serialized)
