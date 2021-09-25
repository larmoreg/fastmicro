import asyncio
from contextlib import asynccontextmanager
from typing import (
    Any,
    AsyncIterator,
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
from fastmicro.messaging import T, HeaderABC, MessagingABC, TopicABC
from fastmicro.serializer import SerializerABC
from fastmicro.serializer.json import Serializer

QT = TypeVar("QT")


class Queue(Generic[QT]):
    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        if loop:
            self.loop = loop
        else:
            self.loop = asyncio.get_event_loop()

        self.nacked: asyncio.Queue[Tuple[bytes, QT]] = asyncio.Queue(loop=self.loop)
        self.queue: asyncio.Queue[Tuple[bytes, QT]] = asyncio.Queue(loop=self.loop)
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
    def __init__(
        self,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        self.queues: Dict[str, Queue[bytes]] = dict()

        super().__init__(loop)

    async def _get_queue(self, topic_name: str) -> Queue[bytes]:
        if topic_name not in self.queues:
            self.queues[topic_name] = Queue(self.loop)
        return self.queues[topic_name]


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

    async def _raw_receive(
        self, queue: Queue[bytes], timeout: Optional[float] = MESSAGING_TIMEOUT
    ) -> Header[T]:
        message_id, serialized = await asyncio.wait_for(queue.get(), timeout=timeout)
        header = await self.deserialize(serialized)
        header.message_id = message_id
        return header

    @asynccontextmanager
    async def _receive_batch(
        self,
        group_name: str,
        consumer_name: str,
        batch_size: int = BATCH_SIZE,
        timeout: Optional[float] = MESSAGING_TIMEOUT,
    ) -> AsyncIterator[Sequence[Header[T]]]:
        queue = await self.messaging._get_queue(self.name)
        tasks = [self._raw_receive(queue) for i in range(batch_size)]
        yield await asyncio.gather(*tasks)

    async def _ack(self, group_name: str, header: HeaderABC[T]) -> None:
        header = cast(Header[T], header)
        queue = await self.messaging._get_queue(self.name)
        await queue.ack(cast(bytes, header.message_id))

    async def _nack(self, group_name: str, header: HeaderABC[T]) -> None:
        header = cast(Header[T], header)
        queue = await self.messaging._get_queue(self.name)
        await queue.nack(cast(bytes, header.message_id))

    async def _send(self, header: HeaderABC[T]) -> None:
        header = cast(Header[T], header)
        queue = await self.messaging._get_queue(self.name)
        serialized = await self.serialize(header)
        await queue.put(serialized)
