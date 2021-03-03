import abc
import asyncio
import os
from typing import Any, Dict, Generic, List, Optional, Tuple, TypeVar
from uuid import UUID, uuid4

import pulsar
import pydantic


class Header(pydantic.BaseModel):
    uuid: UUID = pydantic.Field(default_factory=uuid4)
    parent: Optional[UUID]
    data: Optional[bytes]
    message: Optional[Any]


class Messaging(abc.ABC):
    @abc.abstractmethod
    async def receive(self, topic: str, name: str) -> Header:
        raise NotImplementedError

    @abc.abstractmethod
    async def ack(self, topic: str, name: str, header: Header) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def nack(self, topic: str, name: str, header: Header) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def send(self, topic: str, header: Header) -> None:
        raise NotImplementedError


T = TypeVar("T")


class Queue(Generic[T]):
    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        self.queue: asyncio.Queue[T] = asyncio.Queue(loop=loop)
        self.items: List[T] = list()

    async def get(self, index: int) -> T:
        while index >= len(self.items):
            item = await self.queue.get()
            self.items.append(item)

        return self.items[index]

    async def put(self, item: T) -> None:
        await self.queue.put(item)


class MemoryMessaging(Messaging):
    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        self.loop = loop
        self.lock: asyncio.Lock = asyncio.Lock(loop=self.loop)
        self.queues: Dict[str, Queue[Header]] = dict()
        self.offsets: Dict[str, Dict[str, int]] = dict()

    async def _get_topic_offset(self, topic: str) -> Tuple[Queue[Header], Dict[str, int]]:
        async with self.lock:
            if topic not in self.queues:
                self.queues[topic] = Queue(self.loop)
                self.offsets[topic] = dict()

        return self.queues[topic], self.offsets[topic]

    async def _get_offset(self, offset: Dict[str, int], name: str) -> int:
        async with self.lock:
            if name not in offset:
                offset[name] = 0

        return offset[name]

    async def _increment_offset(self, offset: Dict[str, int], name: str) -> None:
        async with self.lock:
            offset[name] += 1

    async def receive(self, topic: str, name: str) -> Header:
        queue, offset = await self._get_topic_offset(topic)
        index = await self._get_offset(offset, name)

        return await queue.get(index)

    async def ack(self, topic: str, name: str, header: Header) -> None:
        queue, offset = await self._get_topic_offset(topic)
        await self._increment_offset(offset, name)

    async def nack(self, topic: str, name: str, header: Header) -> None:
        pass

    async def send(self, topic: str, header: Header) -> None:
        queue, _ = await self._get_topic_offset(topic)

        queue = self.queues[topic]
        await queue.put(header)


class PulsarMessaging(Messaging):  # pragma: no cover
    def __init__(self, broker_url: str = "pulsar://localhost:6650") -> None:
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
        return Header(uuid=message.message_id(), data=message.data())

    async def ack(self, topic: str, name: str, header: Header) -> None:
        consumer = await self._get_consumer(topic, name)
        consumer.acknowledge(header.uuid)

    async def nack(self, topic: str, name: str, header: Header) -> None:
        consumer = await self._get_consumer(topic, name)
        consumer.negative_acknowledge(header.uuid)

    async def send(self, topic: str, header: Header) -> None:
        producer = await self._get_producer(topic)
        producer.send(header.data)
