import asyncio
import logging
from typing import (
    Dict,
    Generic,
    List,
    Optional,
    Set,
    Tuple,
    Type,
)

from fastmicro.messaging import Messaging
from fastmicro.serializer import Serializer, MsgpackSerializer
from fastmicro.types import T, Header, QT

logger = logging.getLogger(__name__)


class Queue(Generic[QT]):
    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        self.lock: asyncio.Lock = asyncio.Lock(loop=loop)
        self.queue: asyncio.Queue[Tuple[bytes, QT]] = asyncio.Queue(loop=loop)
        self.index: int = 0
        self.items: Dict[bytes, QT] = dict()
        self.pending: Set[bytes] = set()
        self.nacked: List[bytes] = list()

    async def get(self) -> Tuple[bytes, QT]:
        async with self.lock:
            if self.nacked:
                message_id = self.nacked.pop()
            else:
                message_id, item = await self.queue.get()
                self.items[message_id] = item
            self.pending.add(message_id)
        return message_id, self.items[message_id]

    async def put(self, item: QT) -> bytes:
        message_id = str(self.index).encode()
        self.index += 1
        await self.queue.put((message_id, item))
        return message_id

    async def ack(self, message_id: bytes) -> None:
        async with self.lock:
            if message_id in self.pending:
                self.pending.remove(message_id)

    async def nack(self, message_id: bytes) -> None:
        async with self.lock:
            if message_id in self.pending:
                self.pending.remove(message_id)
                self.nacked.insert(0, message_id)


class MemoryHeader(Generic[T], Header[T]):
    message_id: Optional[bytes]


class MemoryMessaging(Generic[T], Messaging[MemoryHeader[T]]):
    header_type = MemoryHeader

    def __init__(
        self,
        serializer: Type[Serializer] = MsgpackSerializer,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        super().__init__(serializer=serializer)
        self.loop = loop
        self.lock: asyncio.Lock = asyncio.Lock(loop=self.loop)
        self.queues: Dict[str, Queue[bytes]] = dict()

    async def _get_queue(self, topic_name: str) -> Queue[bytes]:
        async with self.lock:
            if topic_name not in self.queues:
                self.queues[topic_name] = Queue(self.loop)
            return self.queues[topic_name]

    async def _receive(
        self, topic_name: str, group_name: str, consumer_name: str
    ) -> MemoryHeader[T]:
        queue = await self._get_queue(topic_name)

        message_id, serialized = await queue.get()
        data = await self.serializer.deserialize(serialized)

        header: MemoryHeader[T] = MemoryHeader(**data)
        header.message_id = message_id

        return header

    async def _ack(self, topic_name: str, group_name: str, header: MemoryHeader[T]) -> None:
        queue = await self._get_queue(topic_name)

        assert header.message_id
        await queue.ack(header.message_id)

    async def _nack(self, topic_name: str, group_name: str, header: MemoryHeader[T]) -> None:
        queue = await self._get_queue(topic_name)

        assert header.message_id
        await queue.nack(header.message_id)

    async def _send(self, topic_name: str, header: MemoryHeader[T]) -> None:
        queue = await self._get_queue(topic_name)

        serialized = await self.serializer.serialize(header.dict())
        await queue.put(serialized)
