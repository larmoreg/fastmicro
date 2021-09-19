import aiokafka
import asyncio
import logging
import sys
from typing import (
    Dict,
    List,
    Optional,
    Tuple,
    Type,
)

from fastmicro.env import (
    BATCH_SIZE,
    MESSAGING_TIMEOUT,
    KAFKA_BOOTSTRAP_SERVERS,
)
from fastmicro.messaging import HeaderABC, HT, MessagingABC
from fastmicro.topic import T, Topic

logger = logging.getLogger(__name__)


class Header(HeaderABC):
    partition: Optional[int] = None
    offset: Optional[int] = None


header_topic = Topic("", Header)


class Messaging(MessagingABC):
    header_type: Type[HT] = Header

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

    async def cleanup(self) -> None:
        tasks = [consumer.stop() for consumer in self.consumers.values()]
        await asyncio.gather(*tasks)

        tasks = [producer.stop() for producer in self.producers.values()]
        await asyncio.gather(*tasks)

    async def _get_consumer(
        self, topic_name: str, group_name: str
    ) -> aiokafka.AIOKafkaConsumer:
        key = topic_name, group_name
        if key not in self.consumers:
            consumer = aiokafka.AIOKafkaConsumer(
                topic_name,
                bootstrap_servers=self.bootstrap_servers,
                loop=self.loop,
                group_id=group_name,
                auto_offset_reset="latest",
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
            )
            await producer.start()
            self.producers[topic_name] = producer
        return self.producers[topic_name]

    async def subscribe(self, topic_name: str, group_name: str) -> None:
        await self._get_consumer(topic_name, group_name)

    @staticmethod
    async def _raw_receive(
        topic: Topic[T], temp_message: aiokafka.structs.ConsumerRecord
    ) -> Tuple[Header, T]:
        header = await header_topic.deserialize(temp_message.value)
        header.partition = temp_message.partition
        header.offset = temp_message.offset

        assert header.data
        message = await topic.deserialize(header.data)
        return header, message

    async def _receive(
        self, topic: Topic[T], group_name: str, consumer_name: str
    ) -> Tuple[Header, T]:
        consumer = await self._get_consumer(topic.name, group_name)
        message = await consumer.getone()
        return await self._raw_receive(topic, message)

    async def _receive_batch(
        self,
        topic: Topic[T],
        group_name: str,
        consumer_name: str,
        batch_size: int = BATCH_SIZE,
        timeout: Optional[float] = MESSAGING_TIMEOUT,
    ) -> Tuple[List[Header], List[T]]:
        consumer = await self._get_consumer(topic.name, group_name)
        temp = await consumer.getmany(
            timeout_ms=int(timeout * 1000) if timeout else sys.maxsize,
            max_records=batch_size,
        )

        tasks = [
            self._raw_receive(topic, message)
            for _, messages in temp.items()
            for message in messages
        ]
        results = await asyncio.gather(*tasks)
        output_headers, output_messages = zip(*results)
        return output_headers, output_messages

    async def _ack(self, topic_name: str, group_name: str, header: Header) -> None:
        tp = aiokafka.TopicPartition(topic_name, header.partition)
        assert header.offset is not None
        offsets = {tp: header.offset + 1}

        consumer = await self._get_consumer(topic_name, group_name)
        await consumer.commit(offsets)

    async def _ack_batch(
        self, topic_name: str, group_name: str, headers: List[Header]
    ) -> None:
        partitions = set(map(lambda x: x.partition, headers))
        offsets = {
            aiokafka.TopicPartition(topic_name, partition): max(
                map(
                    lambda x: x.offset + 1,  # type: ignore
                    filter(lambda x: x.partition == partition, headers),
                )
            )
            for partition in partitions
        }

        consumer = await self._get_consumer(topic_name, group_name)
        await consumer.commit(offsets)

    async def _nack(self, topic_name: str, group_name: str, header: Header) -> None:
        pass

    async def _nack_batch(
        self, topic_name: str, group_name: str, headers: List[Header]
    ) -> None:
        pass

    @staticmethod
    async def _raw_send(
        producer: aiokafka.AIOKafkaProducer, topic: Topic[T], header: Header, message: T
    ) -> None:
        serialized = await topic.serialize(message)
        header.data = serialized

        serialized = await header_topic.serialize(header)
        await producer.send_and_wait(topic.name, serialized)

    async def _send(self, topic: Topic[T], header: Header, message: T) -> None:
        producer = await self._get_producer(topic.name)
        await self._raw_send(producer, topic, header, message)

    async def _send_batch(
        self, topic: Topic[T], headers: List[Header], messages: List[T]
    ) -> None:
        producer = await self._get_producer(topic.name)
        tasks = [
            self._raw_send(producer, topic, header, message)
            for header, message in zip(headers, messages)
        ]
        await asyncio.gather(*tasks)
