import asyncio
from typing import (
    cast,
    Dict,
    Generic,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
)

from fastmicro.env import BATCH_SIZE, MESSAGING_TIMEOUT
from fastmicro.messaging import MessagingABC
from fastmicro.messaging.header import T, HeaderABC
from fastmicro.serializer import SerializerABC
from fastmicro.serializer.json import Serializer

QT = TypeVar("QT")


class Queue(Generic[QT]):
    def __init__(self):
        self.nacked: asyncio.Queue[Tuple[bytes, QT]] = asyncio.Queue()
        self.queue: asyncio.Queue[Tuple[bytes, QT]] = asyncio.Queue()
        self.pending: Dict[bytes, QT] = dict()
        self.index: int = 0

    async def get(self) -> Tuple[bytes, QT]:
        try:
            message_id, item = self.nacked.get_nowait()
        except asyncio.QueueEmpty:
            message_id, item = await self.queue.get()
        self.pending[message_id] = item
        return message_id, self.pending[message_id]

    async def put(self, item: QT) -> bytes:
        message_id = str(self.index).encode()
        self.index += 1
        await self.queue.put((message_id, item))
        return message_id

    async def ack(self, message_id: bytes) -> None:
        if message_id in self.pending:
            del self.pending[message_id]

    async def nack(self, message_id: bytes) -> None:
        if message_id in self.pending:
            item = self.pending[message_id]
            del self.pending[message_id]
            await self.nacked.put((message_id, item))


class Header(HeaderABC[T], Generic[T]):
    message_id: Optional[bytes] = None


class Messaging(MessagingABC):
    def header_type(self, schema_type: Type[T]) -> Type[Header[T]]:
        return Header[schema_type]  # type: ignore

    def __init__(
        self,
        serializer_type: Type[SerializerABC] = Serializer,
        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop(),
    ) -> None:
        super().__init__(serializer_type, loop)
        self.queues: Dict[str, Queue[bytes]] = dict()

    async def _get_queue(self, topic_name: str) -> Queue[bytes]:
        if topic_name not in self.queues:
            self.queues[topic_name] = Queue()
        return self.queues[topic_name]

    async def ack(
        self, topic_name: str, group_name: str, headers: Sequence[HeaderABC[T]]
    ) -> None:
        headers = cast(Sequence[Header[T]], headers)
        queue = await self._get_queue(topic_name)
        tasks = [queue.ack(cast(bytes, header.message_id)) for header in headers]
        await asyncio.gather(*tasks)

    async def nack(
        self, topic_name: str, group_name: str, headers: Sequence[HeaderABC[T]]
    ) -> None:
        headers = cast(Sequence[Header[T]], headers)
        queue = await self._get_queue(topic_name)
        tasks = [queue.nack(cast(bytes, header.message_id)) for header in headers]
        await asyncio.gather(*tasks)

    async def _receive(self, queue: Queue[bytes], schema_type: Type[T]) -> Header[T]:
        message_id, serialized = await queue.get()
        header = cast(Header[T], await self.deserialize(serialized, schema_type))
        header.message_id = message_id
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
        queue = await self._get_queue(topic_name)
        tasks = [self._receive(queue, schema_type) for i in range(batch_size)]
        try:
            headers = await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout)
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError(f"Timed out after {timeout} sec")
        return cast(Sequence[Header[T]], headers)

    async def _send(self, queue: Queue[bytes], header: Header[T]) -> None:
        serialized = await self.serialize(header)
        await queue.put(serialized)

    async def send(
        self,
        topic_name: str,
        headers: Sequence[HeaderABC[T]],
    ) -> None:
        headers = cast(Sequence[Header[T]], headers)
        queue = await self._get_queue(topic_name)
        tasks = [self._send(queue, header) for header in headers]
        await asyncio.gather(*tasks)
