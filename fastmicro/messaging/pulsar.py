import asyncio

import logging
import pulsar
from typing import Dict, Optional, Tuple, TypeVar

from fastmicro.env import PULSAR_SERVICE_URL
from fastmicro.messaging import MessageABC, MessagingABC
from fastmicro.topic import Topic

logger = logging.getLogger(__name__)


class Message(MessageABC):
    message_id: Optional[bytes]


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

        self.client = pulsar.Client(service_url)
        self.consumers: Dict[Tuple[str, str], pulsar.Consumer] = dict()
        self.producers: Dict[str, pulsar.Producer] = dict()

    async def cleanup(self) -> None:
        self.client.close()

    def _get_consumer(self, topic_name: str, group_name: str) -> pulsar.Consumer:
        key = topic_name, group_name
        return self.consumers[key]

    def _get_producer(self, topic_name: str) -> pulsar.Producer:
        if topic_name not in self.producers:
            self.producers[topic_name] = self.client.create_producer(topic_name)
        return self.producers[topic_name]

    async def subscribe(self, topic_name: str, group_name: str) -> None:
        key = topic_name, group_name
        if key not in self.consumers:
            self.consumers[key] = self.client.subscribe(topic_name, group_name)

    async def _receive(self, topic: Topic[T], group_name: str, consumer_name: str) -> T:
        consumer = self._get_consumer(topic.name, group_name)
        temp = consumer.receive()
        message = await topic.deserialize(temp.data())
        message.message_id = temp.message_id().serialize()
        return message

    async def _ack(self, topic_name: str, group_name: str, message: T) -> None:
        consumer = self._get_consumer(topic_name, group_name)
        temp = pulsar.MessageId.deserialize(message.message_id)
        consumer.acknowledge(temp)

    async def _nack(self, topic_name: str, group_name: str, message: T) -> None:
        consumer = self._get_consumer(topic_name, group_name)
        temp = pulsar.MessageId.deserialize(message.message_id)
        consumer.negative_acknowledge(temp)

    async def _send(self, topic: Topic[T], message: T) -> None:
        producer = self._get_producer(topic.name)
        serialized = await topic.serialize(message)
        producer.send(serialized)
