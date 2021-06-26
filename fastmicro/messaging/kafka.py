import aiokafka
from aiokafka.errors import IllegalOperation
import asyncio
from contextlib import asynccontextmanager
import logging
from typing import (
    AsyncIterator,
    Dict,
    List,
    Optional,
    Tuple,
    TypeVar,
)
from uuid import uuid4

from fastmicro.env import (
    BATCH_SIZE,
    TIMEOUT,
    KAFKA_BOOTSTRAP_SERVERS,
)
from fastmicro.messaging import Message, Messaging
from fastmicro.topic import Topic

logger = logging.getLogger(__name__)


class KafkaMessage(Message):
    partition: Optional[int]
    offset: Optional[int]


T = TypeVar("T", bound=KafkaMessage)


class KafkaMessaging(Messaging):
    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        super().__init__()
        self.bootstrap_servers = bootstrap_servers
        if loop:
            self.loop = loop
        else:
            self.loop = asyncio.get_event_loop()
        self.consumers: Dict[Tuple[str, str], aiokafka.AIOKafkaConsumer] = dict()
        self.producers: Dict[str, aiokafka.AIOKafkaProducer] = dict()

    async def cleanup(self) -> None:
        tasks = [consumer.stop() for consumer in self.consumers.values()]
        await asyncio.gather(*tasks)

        tasks = [producer.stop() for producer in self.producers.values()]
        await asyncio.gather(*tasks)

    async def _get_consumer(self, topic_name: str, group_name: str) -> aiokafka.AIOKafkaConsumer:
        key = topic_name, group_name
        if key not in self.consumers:
            consumer = aiokafka.AIOKafkaConsumer(
                topic_name,
                bootstrap_servers=self.bootstrap_servers,
                loop=self.loop,
                group_id=group_name,
                enable_auto_commit=False,
                auto_offset_reset="latest",
                isolation_level="read_committed",
            )
            await consumer.start()
            self.consumers[key] = consumer
        return self.consumers[key]

    async def _get_producer(self, topic_name: str) -> aiokafka.AIOKafkaProducer:
        if topic_name not in self.producers:
            producer = aiokafka.AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                loop=self.loop,
                transactional_id=uuid4(),
            )
            await producer.start()
            self.producers[topic_name] = producer
        return self.producers[topic_name]

    async def _subscribe(self, topic_name: str, group_name: str) -> None:
        await self._get_consumer(topic_name, group_name)

    async def _receive(self, topic: Topic[T], group_name: str, consumer_name: str) -> T:
        consumer = await self._get_consumer(topic.name, group_name)
        temp_message = await consumer.getone()

        message = await topic.deserialize(temp_message.value)
        message.partition = temp_message.partition
        message.offset = temp_message.offset
        return message

    async def _receive_batch(
        self,
        topic: Topic[T],
        group_name: str,
        consumer_name: str,
        batch_size: int = BATCH_SIZE,
        timeout: float = TIMEOUT,
    ) -> List[T]:
        consumer = await self._get_consumer(topic.name, group_name)
        temp = await consumer.getmany(timeout_ms=int(timeout * 1000), max_records=batch_size)

        output_messages = list()
        for tp, messages in temp.items():
            for temp_message in messages:
                message = await topic.deserialize(temp_message.value)
                message.partition = temp_message.partition
                message.offset = temp_message.offset
                output_messages.append(message)
        return output_messages

    async def _ack(self, topic_name: str, group_name: str, message: T) -> None:
        tp = aiokafka.TopicPartition(topic_name, message.partition)
        assert message.offset is not None

        try:
            producer = await self._get_producer(topic_name)
            await producer.send_offsets_to_transaction({tp: message.offset + 1}, group_name)
        except IllegalOperation:
            consumer = await self._get_consumer(topic_name, group_name)
            await consumer.commit({tp: message.offset + 1})

    async def _ack_batch(self, topic_name: str, group_name: str, messages: List[T]) -> None:
        partitions = set(map(lambda x: x.partition, messages))
        offsets = {
            aiokafka.TopicPartition(topic_name, partition): max(
                map(
                    lambda x: x.offset + 1,  # type: ignore
                    filter(lambda x: x.partition == partition, messages),
                )
            )
            for partition in partitions
        }

        try:
            producer = await self._get_producer(topic_name)
            await producer.send_offsets_to_transaction(offsets, group_name)
        except IllegalOperation:
            consumer = await self._get_consumer(topic_name, group_name)
            await consumer.commit(offsets)

    async def _nack(self, topic_name: str, group_name: str, message: T) -> None:
        pass

    async def _nack_batch(self, topic_name: str, group_name: str, messages: List[T]) -> None:
        pass

    async def _send(self, topic: Topic[T], message: T) -> None:
        producer = await self._get_producer(topic.name)
        serialized = await topic.serialize(message)
        await producer.send_and_wait(topic.name, serialized)

    async def _send_batch(self, topic: Topic[T], messages: List[T]) -> None:
        producer = await self._get_producer(topic.name)
        for message in messages:
            serialized = await topic.serialize(message)
            await producer.send_and_wait(topic.name, serialized)

    @asynccontextmanager
    async def transaction(self, topic_name) -> AsyncIterator:
        producer = await self._get_producer(topic_name)
        async with producer.transaction():
            yield
