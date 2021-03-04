import abc
import asyncio
import os
from typing import Any, Dict, Generic, List, Optional, Set, Tuple, TypeVar
from uuid import UUID, uuid4

import pulsar
import pydantic

from .registrar import registrar


class Header(pydantic.BaseModel):
    uuid: Optional[UUID]
    parent: Optional[UUID]
    data: Optional[bytes]
    message: Optional[Any]


class Messaging(abc.ABC):
    def __init__(
        self,
        serializer: str = "msgpack",
    ) -> None:
        self.serializer = registrar.get(serializer)

    async def serialize(self, header: Header) -> bytes:
        return await self.serializer.serialize(header.dict(exclude={"uuid", "message"}))

    async def deserialize(self, serialized: bytes) -> Header:
        header = await self.serializer.deserialize(serialized)
        return Header(**header)

    @abc.abstractmethod
    async def receive(self, topic: str, name: str) -> Header:
        raise NotImplementedError

    @abc.abstractmethod
    async def ack(self, topic: str, name: str, uuid: UUID) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def nack(self, topic: str, name: str, uuid: UUID) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def send(self, topic: str, header: Header) -> None:
        raise NotImplementedError


T = TypeVar("T")


class Queue(Generic[T]):
    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        self.lock: asyncio.Lock = asyncio.Lock(loop=loop)
        self.queue: asyncio.Queue[Tuple[UUID, T]] = asyncio.Queue(loop=loop)
        self.items: Dict[UUID, T] = dict()
        self.pending: Set[UUID] = set()
        self.nacked: List[UUID] = list()

    async def get(self) -> Tuple[UUID, T]:
        async with self.lock:
            if self.nacked:
                uuid = self.nacked.pop()
            else:
                uuid, item = await self.queue.get()
                self.items[uuid] = item
            self.pending.add(uuid)
        return uuid, self.items[uuid]

    async def put(self, item: T) -> UUID:
        uuid = uuid4()
        await self.queue.put((uuid, item))
        return uuid

    async def ack(self, uuid: UUID) -> None:
        async with self.lock:
            if uuid in self.pending:
                self.pending.remove(uuid)

    async def nack(self, uuid: UUID) -> None:
        async with self.lock:
            if uuid in self.pending:
                self.pending.remove(uuid)
                self.nacked.insert(0, uuid)


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

    async def _get_queue(self, topic: str) -> Queue[bytes]:
        async with self.lock:
            if topic not in self.queues:
                self.queues[topic] = Queue(self.loop)
            return self.queues[topic]

    async def receive(self, topic: str, name: str) -> Header:
        queue = await self._get_queue(topic)
        uuid, serialized = await queue.get()
        header = await self.deserialize(serialized)
        header.uuid = uuid
        return header

    async def ack(self, topic: str, name: str, uuid: UUID) -> None:
        queue = await self._get_queue(topic)
        await queue.ack(uuid)

    async def nack(self, topic: str, name: str, uuid: UUID) -> None:
        queue = await self._get_queue(topic)
        await queue.nack(uuid)

    async def send(self, topic: str, header: Header) -> None:
        queue = await self._get_queue(topic)
        serialized = await self.serialize(header)
        header.uuid = await queue.put(serialized)


# TODO: test this
class PulsarMessaging(Messaging):  # pragma: no cover
    def __init__(
        self, serializer: str = "msgpack", broker_url: str = "pulsar://localhost:6650"
    ) -> None:
        super().__init__(serializer)
        broker_url = os.getenv("BROKER_URL", broker_url)
        self.client = pulsar.Client(broker_url)
        self.consumers: Dict[Tuple[str, str], pulsar.Consumer] = dict()
        self.producers: Dict[str, pulsar.Producer] = dict()

    async def _get_consumer(self, topic: str, name: str) -> pulsar.Consumer:
        key = topic, name
        if key not in self.consumers:
            self.consumers[key] = self.client.subscribe(topic, name)
        return self.consumers[key]

    async def _get_producer(self, topic: str) -> pulsar.Producer:
        if topic not in self.producers:
            self.producers[topic] = self.client.create_producer(topic)
        return self.producers[topic]

    async def receive(self, topic: str, name: str) -> Header:
        consumer = await self._get_consumer(topic, name)
        message = consumer.receive()
        header = await self.deserialize(message.data())
        header.uuid = message.message_id()
        return header

    async def ack(self, topic: str, name: str, uuid: UUID) -> None:
        consumer = await self._get_consumer(topic, name)
        consumer.acknowledge(uuid)

    async def nack(self, topic: str, name: str, uuid: UUID) -> None:
        consumer = await self._get_consumer(topic, name)
        consumer.negative_acknowledge(uuid)

    async def send(self, topic: str, header: Header) -> None:
        producer = await self._get_producer(topic)
        serialized = await self.serialize(header)
        header.uuid = producer.send(serialized)
