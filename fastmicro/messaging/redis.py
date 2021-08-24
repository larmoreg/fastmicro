import aioredis
import asyncio
from contextlib import asynccontextmanager
import logging
from pydantic import Field
from typing import (
    Any,
    AsyncIterator,
    Dict,
    List,
    Optional,
    TypeVar,
)

from fastmicro.env import (
    BATCH_SIZE,
    MESSAGING_TIMEOUT,
    REDIS_ADDRESS,
)
from fastmicro.messaging import MessageABC, MessagingABC
from fastmicro.topic import Topic

logger = logging.getLogger(__name__)


class Message(MessageABC):
    message_id: Optional[bytes] = Field(None, hidden=True)


T = TypeVar("T", bound=Message)


class Messaging(MessagingABC):
    def __init__(
        self,
        address: str = REDIS_ADDRESS,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        if loop:
            self.loop = loop
        else:
            self.loop = asyncio.get_event_loop()
        super().__init__(self.loop)

        self.address = address
        self.redis: Optional[Any] = None
        self.transactions: Dict[str, Optional[Any]] = dict()

    async def connect(self) -> None:
        self.redis = await aioredis.create_redis_pool(self.address, loop=self.loop)

    async def cleanup(self) -> None:
        assert self.redis
        self.redis.close()
        await self.redis.wait_closed()

    async def _topic_exists(self, topic_name: str) -> bool:
        try:
            assert self.redis
            await self.redis.xinfo_stream(topic_name)
        except aioredis.errors.ReplyError:
            return False
        return True

    async def _group_exists(self, topic_name: str, group_name: str) -> bool:
        assert self.redis
        group_infos = await self.redis.xinfo_groups(topic_name)
        if any(group_info[b"name"] == group_name.encode() for group_info in group_infos):
            return True
        return False

    async def _create_group(self, topic_name: str, group_name: str) -> None:
        if not await self._topic_exists(topic_name) or not await self._group_exists(
            topic_name, group_name
        ):
            assert self.redis
            await self.redis.xgroup_create(topic_name, group_name, latest_id="$", mkstream=True)

    async def subscribe(self, topic_name: str, group_name: str) -> None:
        await self._create_group(topic_name, group_name)

    @staticmethod
    async def _raw_receive(
        topic: Topic[T], temp_message: Dict[bytes, bytes], message_id: bytes
    ) -> T:
        message = await topic.deserialize(temp_message[b"data"])
        message.message_id = message_id
        return message

    async def _receive(self, topic: Topic[T], group_name: str, consumer_name: str) -> T:
        messages = await self._receive_batch(
            topic, group_name, consumer_name, batch_size=1, timeout=0
        )
        return messages[0]

    async def _receive_batch(
        self,
        topic: Topic[T],
        group_name: str,
        consumer_name: str,
        batch_size: int = BATCH_SIZE,
        timeout: float = MESSAGING_TIMEOUT,
    ) -> List[T]:
        assert self.redis
        temp = await self.redis.xread_group(
            group_name,
            consumer_name,
            [topic.name],
            timeout=int(timeout * 1000),
            count=batch_size,
            latest_ids=[">"],
        )

        tasks = [
            self._raw_receive(topic, message, message_id) for stream, message_id, message in temp
        ]
        messages = await asyncio.gather(*tasks)
        return messages

    async def _ack(self, topic_name: str, group_name: str, message: T) -> None:
        await self._ack_batch(topic_name, group_name, [message])

    async def _ack_batch(self, topic_name: str, group_name: str, messages: List[T]) -> None:
        assert self.redis
        message_ids = [message.message_id for message in messages]
        await self.redis.xack(topic_name, group_name, *message_ids)

    async def _nack(self, topic_name: str, group_name: str, message: T) -> None:
        pass

    async def _nack_batch(self, topic_name: str, group_name: str, messages: List[T]) -> None:
        pass

    @staticmethod
    async def _raw_send(transaction: Optional[Any], topic: Topic[T], message: T) -> None:
        assert transaction
        serialized = await topic.serialize(message)
        transaction.xadd(topic.name, {"data": serialized})

    async def _send(self, topic: Topic[T], message: T) -> None:
        async with self.transaction(topic.name) as transaction:
            await self._raw_send(transaction, topic, message)

    async def _send_batch(self, topic: Topic[T], messages: List[T]) -> None:
        async with self.transaction(topic.name) as transaction:
            tasks = [self._raw_send(transaction, topic, message) for message in messages]
            await asyncio.gather(*tasks)

    @asynccontextmanager
    async def transaction(self, topic_name: str) -> AsyncIterator[Optional[Any]]:
        if topic_name not in self.transactions:
            assert self.redis
            transaction = self.redis.multi_exec()
            self.transactions[topic_name] = transaction
            yield transaction
            await transaction.execute()
            del self.transactions[topic_name]
        else:
            yield self.transactions[topic_name]
