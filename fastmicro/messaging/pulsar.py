from aioify import aioify
import asyncio
import logging
import pulsar
from pydantic import Field
from typing import Dict, Optional, Tuple, TypeVar

from fastmicro.env import PULSAR_SERVICE_URL
from fastmicro.messaging import MessageABC, MessagingABC
from fastmicro.topic import Topic

aiopulsar = aioify(obj=pulsar, name="aiopulsar")

logger = logging.getLogger(__name__)


class Message(MessageABC):
    message_id: Optional[bytes] = Field(None, hidden=True)


T = TypeVar("T", bound=Message)


class Messaging(MessagingABC):
    def __init__(
        self,
        service_url: str = PULSAR_SERVICE_URL,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        if loop:
            self.loop = loop
        else:
            self.loop = asyncio.get_event_loop()
        super().__init__(self.loop)

        self.client: aiopulsar.Client = aiopulsar.Client(service_url)  # type: ignore
        self.consumers: Dict[Tuple[str, str], aiopulsar.Consumer] = dict()  # type: ignore
        self.producers: Dict[str, aiopulsar.Producer] = dict()  # type: ignore

    async def cleanup(self) -> None:
        await self.client.close()

    def _get_consumer(self, topic_name: str, group_name: str) -> aiopulsar.Consumer:  # type: ignore
        key = topic_name, group_name
        return self.consumers[key]

    async def _get_producer(self, topic_name: str) -> aiopulsar.Producer:  # type: ignore
        if topic_name not in self.producers:
            producer = await self.client.create_producer(topic_name, batching_enabled=True)
            self.producers[topic_name] = aioify(obj=producer, name="aioproducer")
        return self.producers[topic_name]

    async def subscribe(self, topic_name: str, group_name: str) -> None:
        key = topic_name, group_name
        if key not in self.consumers:
            consumer = await self.client.subscribe(topic_name, group_name)
            self.consumers[key] = aioify(obj=consumer, name="aioconsumer")

    async def _receive(self, topic: Topic[T], group_name: str, consumer_name: str) -> T:
        consumer = self._get_consumer(topic.name, group_name)
        temp = await consumer.receive()
        message = await topic.deserialize(temp.data())
        message.message_id = temp.message_id().serialize()
        return message

    async def _ack(self, topic_name: str, group_name: str, message: T) -> None:
        consumer = self._get_consumer(topic_name, group_name)
        temp = await aiopulsar.MessageId.deserialize(message.message_id)
        await consumer.acknowledge(temp)

    async def _nack(self, topic_name: str, group_name: str, message: T) -> None:
        consumer = self._get_consumer(topic_name, group_name)
        temp = await aiopulsar.MessageId.deserialize(message.message_id)
        await consumer.negative_acknowledge(temp)

    async def _send(self, topic: Topic[T], message: T) -> None:
        producer = await self._get_producer(topic.name)
        serialized = await topic.serialize(message)
        await producer.send(serialized)
