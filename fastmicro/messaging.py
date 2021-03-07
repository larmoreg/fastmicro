import abc
import asyncio
import atexit
import os
from typing import Any, Dict, Generic, List, Optional, Set, Tuple, TypeVar
from uuid import UUID, uuid4

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

    async def serialize(self, header: Header) -> bytes:
        return await self.serializer.serialize(header.dict(exclude={"id", "message"}))

    async def deserialize(self, serialized: bytes) -> Header:
        header = await self.serializer.deserialize(serialized)
        return Header(**header)

    @abc.abstractmethod
    async def receive(self, topic: str, name: str) -> Header:
        raise NotImplementedError

    @abc.abstractmethod
    async def ack(self, topic: str, name: str, id: bytes) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def nack(self, topic: str, name: str, id: bytes) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def send(self, topic: str, header: Header) -> None:
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
                id = self.nacked.pop()
            else:
                id, item = await self.queue.get()
                self.items[id] = item
            self.pending.add(id)
        return id, self.items[id]

    async def put(self, item: T) -> bytes:
        id = str(self.index).encode()
        self.index += 1
        await self.queue.put((id, item))
        return id

    async def ack(self, id: bytes) -> None:
        async with self.lock:
            if id in self.pending:
                self.pending.remove(id)

    async def nack(self, id: bytes) -> None:
        async with self.lock:
            if id in self.pending:
                self.pending.remove(id)
                self.nacked.insert(0, id)


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
        id, serialized = await queue.get()
        header = await self.deserialize(serialized)
        header.id = id
        return header

    async def ack(self, topic: str, name: str, id: bytes) -> None:
        queue = await self._get_queue(topic)
        await queue.ack(id)

    async def nack(self, topic: str, name: str, id: bytes) -> None:
        queue = await self._get_queue(topic)
        await queue.nack(id)

    async def send(self, topic: str, header: Header) -> None:
        queue = await self._get_queue(topic)
        header.uuid = uuid4()
        serialized = await self.serialize(header)
        header.id = await queue.put(serialized)


class PulsarMessaging(Messaging):  # pragma: no cover
    def __init__(
        self,
        serializer: str = "msgpack",
        broker_url: str = "pulsar://localhost:6650",
        *args,
        **kwargs
    ) -> None:
        super().__init__(serializer)
        broker_url = os.getenv("BROKER_URL", broker_url)
        self.client = pulsar.Client(broker_url, *args, **kwargs)
        self.consumers: Dict[Tuple[str, str], pulsar.Consumer] = dict()
        self.producers: Dict[str, pulsar.Producer] = dict()
        atexit.register(self.cleanup)

    def cleanup(self) -> None:
        self.client.close()

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
        header.id = message.message_id().serialize()
        return header

    async def ack(self, topic: str, name: str, id: bytes) -> None:
        temp = pulsar.MessageId.deserialize(id)
        consumer = await self._get_consumer(topic, name)
        consumer.acknowledge(temp)

    async def nack(self, topic: str, name: str, id: bytes) -> None:
        temp = pulsar.MessageId.deserialize(id)
        consumer = await self._get_consumer(topic, name)
        consumer.negative_acknowledge(temp)

    async def send(self, topic: str, header: Header) -> None:
        producer = await self._get_producer(topic)
        header.uuid = uuid4()
        serialized = await self.serialize(header)
        producer.send(serialized)
