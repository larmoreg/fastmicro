import asyncio
import logging
from typing import (
    Dict,
    Generic,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

from fastmicro.messaging import HeaderABC, HT, MessagingABC
from fastmicro.topic import T, Topic

logger = logging.getLogger(__name__)

QT = TypeVar("QT")


class Queue(Generic[QT]):
    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
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


class Header(HeaderABC):
    message_id: Optional[bytes] = None


header_topic = Topic("", Header)


class Messaging(MessagingABC):
    header_type: Type[HT] = Header

    def __init__(
        self,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        if loop:
            self.loop = loop
        else:
            self.loop = asyncio.get_event_loop()
        super().__init__(self.loop)

        self.queues: Dict[str, Queue[bytes]] = dict()

    async def _get_queue(self, topic_name: str) -> Queue[bytes]:
        if topic_name not in self.queues:
            self.queues[topic_name] = Queue(self.loop)
        return self.queues[topic_name]

    async def _receive(
        self, topic: Topic[T], group_name: str, consumer_name: str
    ) -> Tuple[Header, T]:
        queue = await self._get_queue(topic.name)
        message_id, serialized = await queue.get()
        header = await header_topic.deserialize(serialized)
        header.message_id = message_id

        assert header.data
        message = await topic.deserialize(header.data)
        return header, message

    async def _ack(self, topic_name: str, group_name: str, header: Header) -> None:
        queue = await self._get_queue(topic_name)
        assert header.message_id
        await queue.ack(header.message_id)

    async def _nack(self, topic_name: str, group_name: str, header: Header) -> None:
        queue = await self._get_queue(topic_name)
        assert header.message_id
        await queue.nack(header.message_id)

    async def _send(self, topic: Topic[T], header: Header, message: T) -> None:
        queue = await self._get_queue(topic.name)
        serialized = await topic.serialize(message)
        header.data = serialized

        serialized = await header_topic.serialize(header)
        await queue.put(serialized)
