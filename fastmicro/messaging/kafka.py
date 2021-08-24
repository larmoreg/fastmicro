import aiokafka
from aiokafka.errors import IllegalOperation
import asyncio
from contextlib import asynccontextmanager
import logging
from pydantic import Field
import sys
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
    MESSAGING_TIMEOUT,
    KAFKA_BOOTSTRAP_SERVERS,
)
from fastmicro.messaging import MessageABC, MessagingABC
from fastmicro.topic import Topic

logger = logging.getLogger(__name__)


class Message(MessageABC):
    partition: Optional[int] = Field(None, hidden=True)
    offset: Optional[int] = Field(None, hidden=True)


T = TypeVar("T", bound=Message)


class Messaging(MessagingABC):
    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        if loop:
            self.loop = loop
        else:
            self.loop = asyncio.get_event_loop()
        super().__init__(self.loop)

        self.bootstrap_servers = bootstrap_servers
        self.consumers: Dict[Tuple[str, str], aiokafka.AIOKafkaConsumer] = dict()
        self.producers: Dict[str, aiokafka.AIOKafkaProducer] = dict()
        self.transactions: Dict[str, aiokafka.TransactionContext] = dict()

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

    async def subscribe(self, topic_name: str, group_name: str) -> None:
        await self._get_consumer(topic_name, group_name)

    @staticmethod
    async def _raw_receive(topic: Topic[T], temp_message: aiokafka.structs.ConsumerRecord) -> T:
        message = await topic.deserialize(temp_message.value)
        message.partition = temp_message.partition
        message.offset = temp_message.offset
        return message

    async def _receive(self, topic: Topic[T], group_name: str, consumer_name: str) -> T:
        consumer = await self._get_consumer(topic.name, group_name)
        message = await consumer.getone()
        return await self._raw_receive(topic, message)

    async def _receive_batch(
        self,
        topic: Topic[T],
        group_name: str,
        consumer_name: str,
        batch_size: int = BATCH_SIZE,
        timeout: float = MESSAGING_TIMEOUT,
    ) -> List[T]:
        consumer = await self._get_consumer(topic.name, group_name)
        timeout_ms = int(timeout * 1000)
        if not timeout_ms:
            timeout_ms = sys.maxsize
        temp = await consumer.getmany(timeout_ms=timeout_ms, max_records=batch_size)

        tasks = [
            self._raw_receive(topic, message)
            for _, messages in temp.items()
            for message in messages
        ]
        output_messages = await asyncio.gather(*tasks)
        return output_messages

    async def _ack(self, topic_name: str, group_name: str, message: T) -> None:
        tp = aiokafka.TopicPartition(topic_name, message.partition)
        assert message.offset is not None
        offsets = {tp: message.offset + 1}

        try:
            producer = await self._get_producer(topic_name)
            await producer.send_offsets_to_transaction(offsets, group_name)
        except IllegalOperation:
            consumer = await self._get_consumer(topic_name, group_name)
            await consumer.commit(offsets)

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

    @staticmethod
    async def _raw_send(producer: aiokafka.AIOKafkaProducer, topic: Topic[T], message: T) -> None:
        serialized = await topic.serialize(message)
        await producer.send(topic.name, serialized)

    async def _send(self, topic: Topic[T], message: T) -> None:
        producer = await self._get_producer(topic.name)
        async with self.transaction(topic.name):
            await self._raw_send(producer, topic, message)

    async def _send_batch(self, topic: Topic[T], messages: List[T]) -> None:
        producer = await self._get_producer(topic.name)
        async with self.transaction(topic.name):
            tasks = [self._raw_send(producer, topic, message) for message in messages]
            await asyncio.gather(*tasks)

    @asynccontextmanager
    async def transaction(self, topic_name: str) -> AsyncIterator[None]:
        if topic_name not in self.transactions:
            producer = await self._get_producer(topic_name)
            transaction = producer.transaction()
            self.transactions[topic_name] = transaction
            async with transaction:
                yield
            del self.transactions[topic_name]
        else:
            yield
