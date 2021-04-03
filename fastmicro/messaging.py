import abc
import asyncio
from contextlib import asynccontextmanager
import logging
import os
from typing import (
    Any,
    AsyncIterator,
    Dict,
    Generic,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)
from uuid import uuid4

import aioredis
import pydantic

from .serializer import Serializer, MsgpackSerializer
from .topic import Header, Topic

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=pydantic.BaseModel)


class Messaging(abc.ABC):
    def __init__(
        self,
        serializer: Type[Serializer] = MsgpackSerializer,
    ):
        self.serializer = serializer()

    async def connect(self) -> None:
        pass

    async def cleanup(self) -> None:
        pass

    async def _subscribe(self, topic_name: str, group_name: str) -> None:
        pass

    @abc.abstractmethod
    async def _receive(self, topic_name: str, group_name: str, consumer_name: str) -> Header:
        raise NotImplementedError

    @abc.abstractmethod
    async def _ack(self, topic_name: str, group_name: str, header: Any) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def _nack(self, topic_name: str, group_name: str, header: Any) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def _send(self, topic_name: str, header: Any) -> None:
        raise NotImplementedError

    @asynccontextmanager
    async def receive(
        self, topic: Topic[T], group_name: str, consumer_name: str
    ) -> AsyncIterator[Header]:
        try:
            await self._subscribe(topic.name, group_name)
            header = await self._receive(topic.name, group_name, consumer_name)
            logger.debug(f"Received {header.uuid}")
            assert header.data
            header.message = await topic.deserialize(header.data)

            yield header

            logger.debug(f"Acking {header.uuid}")
            await self._ack(topic.name, group_name, header)
        except Exception:
            logger.exception("Processing failed")

            logger.debug(f"Nacking {header.uuid}")
            await self._nack(topic.name, group_name, header)

    async def send(self, topic: Topic[T], message: Union[Header, Any]) -> Header:
        if isinstance(message, Header):
            header = message
        else:
            header = Header(message=message)

        header.uuid = uuid4()
        header.data = await topic.serialize(header.message)

        logger.debug(f"Sending {header.uuid}")
        await self._send(topic.name, header)
        return header


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


class MemoryHeader(Header):
    message_id: Optional[bytes]


class MemoryMessaging(Messaging):
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

    async def _receive(self, topic_name: str, group_name: str, consumer_name: str) -> MemoryHeader:
        queue = await self._get_queue(topic_name)
        message_id, serialized = await queue.get()
        data = await self.serializer.deserialize(serialized)

        header = MemoryHeader(**data)
        header.message_id = message_id
        return header

    async def _ack(self, topic_name: str, group_name: str, header: MemoryHeader) -> None:
        queue = await self._get_queue(topic_name)
        assert header.message_id
        await queue.ack(header.message_id)

    async def _nack(self, topic_name: str, group_name: str, header: MemoryHeader) -> None:
        queue = await self._get_queue(topic_name)
        assert header.message_id
        await queue.nack(header.message_id)

    async def _send(self, topic_name: str, header: MemoryHeader) -> None:
        queue = await self._get_queue(topic_name)
        serialized = await self.serializer.serialize(header.dict())
        await queue.put(serialized)


class RedisHeader(Header):
    message_id: Optional[bytes]


class RedisMessaging(Messaging):  # pragma: no cover
    def __init__(
        self,
        serializer: Type[Serializer] = MsgpackSerializer,
        address: str = "redis://localhost:6379",
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        super().__init__(serializer=serializer)
        self.address = os.getenv("ADDRESS", address)
        if loop:
            self.loop = loop
        else:
            self.loop = asyncio.get_event_loop()
        self.pool = None

    async def connect(self) -> None:
        self.pool = await aioredis.create_pool(self.address, loop=self.loop)

    async def cleanup(self) -> None:
        assert self.pool
        self.pool.close()
        await self.pool.wait_closed()

    async def _topic_exists(self, topic_name: str) -> bool:
        try:
            assert self.pool
            with await self.pool as connection:
                redis = aioredis.Redis(connection)
                await redis.xinfo_stream(topic_name)
        except aioredis.errors.ReplyError:
            return False
        return True

    async def _group_exists(self, topic_name: str, group_name: str) -> bool:
        assert self.pool
        with await self.pool as connection:
            redis = aioredis.Redis(connection)
            groups_info = await redis.xinfo_groups(topic_name)
            for group_info in groups_info:
                if group_info[b"name"] == group_name.encode():
                    return True
            return False

    async def _create_group(self, topic_name: str, group_name: str) -> None:
        if not await self._topic_exists(topic_name) or not await self._group_exists(
            topic_name, group_name
        ):
            assert self.pool
            with await self.pool as connection:
                redis = aioredis.Redis(connection)
                await redis.xgroup_create(topic_name, group_name, latest_id="$", mkstream=True)

    async def _subscribe(self, topic_name: str, group_name: str) -> None:
        await self._create_group(topic_name, group_name)

    async def _receive(self, topic_name: str, group_name: str, consumer_name: str) -> RedisHeader:
        assert self.pool
        with await self.pool as connection:
            redis = aioredis.Redis(connection)
            messages = await redis.xread_group(
                group_name, consumer_name, [topic_name], latest_ids=[">"]
            )
            stream, message_id, message = messages[-1]
            data = await self.serializer.deserialize(message[b"data"])

            header = RedisHeader(**data)
            header.message_id = message_id
            return header

    async def _ack(self, topic_name: str, group_name: str, header: RedisHeader) -> None:
        assert self.pool
        with await self.pool as connection:
            redis = aioredis.Redis(connection)
            await redis.xack(topic_name, group_name, header.message_id)

    async def _nack(self, topic_name: str, group_name: str, header: RedisHeader) -> None:
        pass

    async def _send(self, topic_name: str, header: RedisHeader) -> None:
        assert self.pool
        with await self.pool as connection:
            redis = aioredis.Redis(connection)
            serialized = await self.serializer.serialize(header.dict())
            await redis.xadd(topic_name, {"data": serialized})
