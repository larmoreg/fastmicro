import asyncio
import logging
from typing import (
    Dict,
    Generic,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
)

from fastmicro.messaging import MessageABC, MessagingABC
from fastmicro.topic import Topic

logger = logging.getLogger(__name__)

QT = TypeVar("QT")


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


class Message(MessageABC):
    message_id: Optional[bytes]


T = TypeVar("T", bound=Message)


class Messaging(MessagingABC):
    def __init__(
        self,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        if loop:
            self.loop = loop
        else:
            self.loop = asyncio.get_event_loop()
        super().__init__(self.loop)

        self.lock: asyncio.Lock = asyncio.Lock(loop=self.loop)
        self.queues: Dict[str, Queue[bytes]] = dict()

    async def _get_queue(self, topic_name: str) -> Queue[bytes]:
        async with self.lock:
            if topic_name not in self.queues:
                self.queues[topic_name] = Queue(self.loop)
            return self.queues[topic_name]

    async def _receive(self, topic: Topic[T], group_name: str, consumer_name: str) -> T:
        queue = await self._get_queue(topic.name)
        message_id, serialized = await queue.get()
        message = await topic.deserialize(serialized)
        message.message_id = message_id
        return message

    async def _ack(self, topic_name: str, group_name: str, message: T) -> None:
        queue = await self._get_queue(topic_name)
        assert message.message_id
        await queue.ack(message.message_id)

    async def _nack(self, topic_name: str, group_name: str, message: T) -> None:
        queue = await self._get_queue(topic_name)
        assert message.message_id
        await queue.nack(message.message_id)

    async def _send(self, topic: Topic[T], message: T) -> None:
        queue = await self._get_queue(topic.name)
        serialized = await topic.serialize(message)
        await queue.put(serialized)
