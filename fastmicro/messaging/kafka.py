import aiokafka
import asyncio
from contextlib import asynccontextmanager
import sys
from typing import (
    Any,
    AsyncIterator,
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
from fastmicro.messaging import T, HeaderABC, MessagingABC, TopicABC
from fastmicro.serializer import SerializerABC
from fastmicro.serializer.json import Serializer


class Header(HeaderABC[T], Generic[T]):
    partition: Optional[int] = None
    offset: Optional[int] = None


class Messaging(MessagingABC):
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
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        super().__init__(loop)

    async def connect(self) -> None:
        self.lock = asyncio.Lock(loop=self.loop)
        self.listener = self.ConsumerRebalanceListener(self.lock)
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

    async def _get_producer(self, topic_name: str) -> aiokafka.AIOKafkaProducer:
        if topic_name not in self.producers:
            producer = aiokafka.AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                loop=self.loop,
            )
            await producer.start()
            self.producers[topic_name] = producer
        return self.producers[topic_name]


class Topic(TopicABC[T], Generic[T]):
    def header(self, **kwargs: Any) -> Header[T]:
        T = self.__orig_class__.__args__[0]  # type: ignore
        return Header[T](**kwargs)  # type: ignore

    def __init__(
        self,
        name: str,
        messaging: Messaging,
        serializer_type: Type[SerializerABC] = Serializer,
    ):
        self.name = name
        self.messaging: Messaging = messaging
        self.serializer_type = serializer_type

    async def serialize(self, header: HeaderABC[T]) -> bytes:
        return await self.serializer_type.serialize(header.dict())

    async def deserialize(self, serialized: bytes) -> Header[T]:
        data = await self.serializer_type.deserialize(serialized)
        return self.header(**data)

    async def subscribe(self, group_name: str) -> None:
        await self.messaging._get_consumer(self.name, group_name)

    async def _raw_receive(
        self,
        temp_message: aiokafka.structs.ConsumerRecord,
    ) -> HeaderABC[T]:
        header = await self.deserialize(temp_message.value)
        header.partition = temp_message.partition
        header.offset = temp_message.offset
        return header

    @asynccontextmanager
    async def _receive(
        self,
        group_name: str,
        consumer_name: str,
        timeout: Optional[float] = MESSAGING_TIMEOUT,
    ) -> AsyncIterator[HeaderABC[T]]:
        consumer = await self.messaging._get_consumer(self.name, group_name)
        message = await asyncio.wait_for(consumer.getone(), timeout=timeout)
        await self.messaging.lock.acquire()
        yield await self._raw_receive(message)
        self.messaging.lock.release()

    @asynccontextmanager
    async def _receive_batch(
        self,
        group_name: str,
        consumer_name: str,
        batch_size: int = BATCH_SIZE,
        timeout: Optional[float] = MESSAGING_TIMEOUT,
    ) -> AsyncIterator[Sequence[HeaderABC[T]]]:
        consumer = await self.messaging._get_consumer(self.name, group_name)
        temp = await consumer.getmany(
            timeout_ms=int(timeout * 1000) if timeout is not None else sys.maxsize,
            max_records=batch_size,
        )

        await self.messaging.lock.acquire()
        tasks = [
            self._raw_receive(message)
            for _, messages in temp.items()
            for message in messages
        ]
        if not tasks:
            raise asyncio.TimeoutError

        yield await asyncio.gather(*tasks)
        self.messaging.lock.release()

    async def _ack(self, group_name: str, header: HeaderABC[T]) -> None:
        header = cast(Header[T], header)
        tp = aiokafka.TopicPartition(self.name, header.partition)
        offsets = {tp: cast(int, header.offset) + 1}

        consumer = await self.messaging._get_consumer(self.name, group_name)
        await consumer.commit(offsets)

    async def _ack_batch(
        self, group_name: str, headers: Sequence[HeaderABC[T]]
    ) -> None:
        headers = cast(Sequence[Header[T]], headers)
        partitions = set(map(lambda x: x.partition, headers))
        offsets = {
            aiokafka.TopicPartition(self.name, partition): max(
                map(
                    lambda x: x.offset + 1,  # type: ignore
                    filter(lambda x: x.partition == partition, headers),
                )
            )
            for partition in partitions
        }

        consumer = await self.messaging._get_consumer(self.name, group_name)
        await consumer.commit(offsets)

    async def _nack(self, group_name: str, header: HeaderABC[T]) -> None:
        pass

    async def _nack_batch(
        self, group_name: str, headers: Sequence[HeaderABC[T]]
    ) -> None:
        pass

    async def _raw_send(
        self,
        producer: aiokafka.AIOKafkaProducer,
        header: HeaderABC[T],
    ) -> None:
        serialized = await self.serialize(header)
        await producer.send_and_wait(self.name, serialized)

    async def _send(self, header: HeaderABC[T]) -> None:
        producer = await self.messaging._get_producer(self.name)
        await self._raw_send(producer, header)

    async def _send_batch(self, headers: Sequence[HeaderABC[T]]) -> None:
        producer = await self.messaging._get_producer(self.name)
        tasks = [self._raw_send(producer, header) for header in headers]
        await asyncio.gather(*tasks)
