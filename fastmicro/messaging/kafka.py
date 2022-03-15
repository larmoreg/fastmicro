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
    List,
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


class Consumer(aiokafka.AIOKafkaConsumer):  # type: ignore
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
        *args: Any,
        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop(),
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.lock = asyncio.Lock(loop=loop)

    def subscribe(
        self,
        *args: Any,
        listener: Optional[aiokafka.ConsumerRebalanceListener] = None,
        **kwargs: Any,
    ) -> None:
        if not listener:
            listener = self.ConsumerRebalanceListener(self.lock)
        super().subscribe(*args, listener=listener, **kwargs)


class Messaging(MessagingABC):
    def header_type(self, schema_type: Type[T]) -> Type[Header[T]]:
        return Header[schema_type]  # type: ignore

    def __init__(
        self,
        *,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        serializer_type: Type[SerializerABC] = Serializer,
        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop(),
    ) -> None:
        super().__init__(serializer_type=serializer_type, loop=loop)
        self.bootstrap_servers = bootstrap_servers
        self.initialized = False

    async def _create_consumer(
        self, topic_name: str, group_name: str, auto_offset_reset: str
    ) -> None:
        key = topic_name, group_name
        if key not in self.consumers:
            consumer = Consumer(
                bootstrap_servers=self.bootstrap_servers,
                loop=self.loop,
                group_id=group_name,
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=False,
                isolation_level="read_committed",
            )
            consumer.subscribe([topic_name])
            await consumer.start()

            self.consumers[key] = consumer

    async def connect(self) -> None:
        if not self.initialized:
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

    async def subscribe(
        self, topic_name: str, group_name: str, latest: bool = False
    ) -> None:
        await self._create_consumer(
            topic_name, group_name, "latest" if latest else "earliest"
        )

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

        key = topic_name, group_name
        consumer = self.consumers[key]
        await consumer.commit(offsets)

    async def nack(
        self, topic_name: str, group_name: str, headers: Sequence[HeaderABC[T]]
    ) -> None:
        pass

    async def _deserialize(
        self,
        raw_message: aiokafka.structs.ConsumerRecord,
        schema_type: Type[T],
    ) -> Header[T]:
        header = cast(Header[T], await self.deserialize(raw_message.value, schema_type))
        header.partition = raw_message.partition
        header.offset = raw_message.offset
        return header

    async def _receive_batch(
        self,
        consumer: Consumer,
        schema_type: Type[T],
        batch_size: int,
        timeout: Optional[float],
    ) -> Sequence[Header[T]]:
        temp = await consumer.getmany(
            timeout_ms=int(timeout * 1000) if timeout is not None else sys.maxsize,
            max_records=batch_size,
        )
        tasks = [
            self._deserialize(message, schema_type)
            for _, messages in temp.items()
            for message in messages
        ]
        headers = await asyncio.gather(*tasks)
        return cast(Sequence[Header[T]], headers)

    @asynccontextmanager
    async def receive(
        self,
        topic_name: str,
        group_name: str,
        consumer_name: str,
        schema_type: Type[T],
        *,
        batch_size: int = BATCH_SIZE,
        timeout: Optional[float] = MESSAGING_TIMEOUT,
    ) -> AsyncIterator[Sequence[Header[T]]]:
        key = topic_name, group_name
        consumer = self.consumers[key]
        if timeout is not None:
            sleep: asyncio.Task[None] = asyncio.create_task(asyncio.sleep(timeout))

            done = False
            headers: List[Header[T]] = list()
            while not done and len(headers) < batch_size:
                receive = self._receive_batch(
                    consumer, schema_type, batch_size - len(headers), timeout
                )
                complete, pending = await asyncio.wait(
                    (receive, sleep), return_when=asyncio.FIRST_COMPLETED
                )
                for task in complete:
                    if task == sleep:
                        done = True
                    else:
                        temp = task.result()
                        if temp:
                            headers.extend(temp)
        else:
            headers = list()
            while len(headers) < batch_size:
                headers.extend(
                    await self._receive_batch(
                        consumer, schema_type, batch_size - len(headers), timeout
                    )
                )

        await consumer.lock.acquire()
        yield headers
        consumer.lock.release()

    async def _send(
        self,
        topic_name: str,
        header: Header[T],
    ) -> None:
        serialized = await self.serialize(header)
        await self.producer.send_and_wait(topic_name, serialized)

    async def send(self, topic_name: str, headers: Sequence[HeaderABC[T]]) -> None:
        headers = cast(Sequence[Header[T]], headers)
        tasks = [self._send(topic_name, header) for header in headers]
        await asyncio.gather(*tasks)
