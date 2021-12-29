import aiokafka
import asyncio
import sys
from typing import (
    cast,
    Dict,
    Generic,
    Optional,
    Set,
    Sequence,
    Tuple,
    Type,
)

from fastmicro.env import (
    BATCH_SIZE,
    MESSAGING_TIMEOUT,
    KAFKA_BOOTSTRAP_SERVERS,
)
from fastmicro.messaging import MessagingABC
from fastmicro.messaging.header import T, HeaderABC
from fastmicro.serializer import SerializerABC
from fastmicro.serializer.json import Serializer


class Header(HeaderABC[T], Generic[T]):
    partition: Optional[int] = None
    offset: Optional[int] = None


class Messaging(MessagingABC):
    def header_type(self, schema_type: Type[T]) -> Type[Header[T]]:
        return Header[schema_type]  # type: ignore

    class ConsumerRebalanceListener(aiokafka.ConsumerRebalanceListener):  # type: ignore
        def __init__(self, lock: asyncio.Lock):
            self.lock = lock

        async def on_partitions_revoked(
            self, revoked: Set[aiokafka.TopicPartition]
        ) -> None:
            await self.lock.acquire()

        async def on_partitions_assigned(
            self, assigned: Set[aiokafka.TopicPartition]
        ) -> None:
            self.lock.release()

    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        serializer_type: Type[SerializerABC] = Serializer,
        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop(),
    ) -> None:
        super().__init__(serializer_type, loop)
        self.bootstrap_servers = bootstrap_servers
        self.initialized = False

    async def _get_consumer(
        self, topic_name: str, group_name: str
    ) -> aiokafka.AIOKafkaConsumer:
        key = topic_name, group_name
        if key not in self.consumers:
            consumer = aiokafka.AIOKafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                loop=self.loop,
                group_id=group_name,
                auto_offset_reset="latest",
                enable_auto_commit=False,
                isolation_level="read_committed",
            )
            consumer.subscribe([topic_name], listener=self.listener)
            await consumer.start()
            self.consumers[key] = consumer
        return self.consumers[key]

    async def connect(self) -> None:
        if not self.initialized:
            self.lock = asyncio.Lock(loop=self.loop)
            self.listener = self.ConsumerRebalanceListener(self.lock)
            self.consumers: Dict[Tuple[str, str], aiokafka.AIOKafkaConsumer] = dict()

            self.producer = aiokafka.AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                loop=self.loop,
            )
            await self.producer.start()
            self.initialized = True

    async def cleanup(self) -> None:
        if self.initialized:
            tasks = [consumer.stop() for consumer in self.consumers.values()]
            await asyncio.gather(*tasks)

            await self.producer.stop()
            self.initialized = False

    async def subscribe(self, topic_name: str, group_name: str) -> None:
        await self._get_consumer(topic_name, group_name)

    async def ack(
        self, topic_name: str, group_name: str, headers: Sequence[HeaderABC[T]]
    ) -> None:
        headers = cast(Sequence[Header[T]], headers)
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

    async def nack(
        self, topic_name: str, group_name: str, headers: Sequence[HeaderABC[T]]
    ) -> None:
        pass

    async def _receive(
        self,
        raw_message: aiokafka.structs.ConsumerRecord,
        schema_type: Type[T],
    ) -> Header[T]:
        header = cast(Header[T], await self.deserialize(raw_message.value, schema_type))
        header.partition = raw_message.partition
        header.offset = raw_message.offset
        return header

    async def receive(
        self,
        topic_name: str,
        group_name: str,
        consumer_name: str,
        schema_type: Type[T],
        batch_size: int = BATCH_SIZE,
        timeout: Optional[float] = MESSAGING_TIMEOUT,
    ) -> Sequence[Header[T]]:
        consumer = await self._get_consumer(topic_name, group_name)
        temp = await consumer.getmany(
            timeout_ms=int(timeout * 1000) if timeout is not None else sys.maxsize,
            max_records=batch_size,
        )
        if not temp.items():
            raise asyncio.TimeoutError(f"Timed out after {timeout} sec")

        await self.lock.acquire()
        tasks = [
            self._receive(message, schema_type)
            for _, messages in temp.items()
            for message in messages
        ]
        headers = await asyncio.gather(*tasks)
        self.lock.release()
        return cast(Sequence[Header[T]], headers)

    async def _send(
        self,
        topic_name: str,
        header: Header[T],
    ) -> None:
        serialized = await self.serialize(header)
        await self.producer.send_and_wait(topic_name, serialized)

    async def send(
        self, topic_name: str, headers: Sequence[HeaderABC[T]]
    ) -> None:
        headers = cast(Sequence[Header[T]], headers)
        tasks = [self._send(topic_name, header) for header in headers]
        await asyncio.gather(*tasks)
