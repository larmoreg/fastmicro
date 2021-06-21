import asyncio
import logging
from typing import (
    Generic,
    Optional,
    Type,
)

import aioredis

from fastmicro.env import (
    REDIS_ADDRESS,
)
from fastmicro.messaging import Messaging
from fastmicro.serializer import Serializer, MsgpackSerializer
from fastmicro.types import T, Header

logger = logging.getLogger(__name__)


class RedisHeader(Generic[T], Header[T]):
    message_id: Optional[bytes]


class RedisMessaging(Generic[T], Messaging[RedisHeader[T]]):
    header_type = RedisHeader

    def __init__(
        self,
        serializer: Type[Serializer] = MsgpackSerializer,
        address: str = REDIS_ADDRESS,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        super().__init__(serializer=serializer)
        self.address = address
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
            group_infos = await redis.xinfo_groups(topic_name)
            if any(group_info[b"name"] == group_name.encode() for group_info in group_infos):
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

    async def _receive(
        self, topic_name: str, group_name: str, consumer_name: str
    ) -> RedisHeader[T]:
        assert self.pool
        with await self.pool as connection:
            redis = aioredis.Redis(connection)
            messages = await redis.xread_group(
                group_name, consumer_name, [topic_name], count=1, latest_ids=[">"]
            )
            stream, message_id, message = messages[0]
            data = await self.serializer.deserialize(message[b"data"])

            header: RedisHeader[T] = RedisHeader(**data)
            header.message_id = message_id
            return header

    async def _ack(self, topic_name: str, group_name: str, header: RedisHeader[T]) -> None:
        assert self.pool
        with await self.pool as connection:
            redis = aioredis.Redis(connection)
            await redis.xack(topic_name, group_name, header.message_id)

    async def _nack(self, topic_name: str, group_name: str, header: RedisHeader[T]) -> None:
        pass

    async def _send(self, topic_name: str, header: RedisHeader[T]) -> None:
        assert self.pool
        with await self.pool as connection:
            redis = aioredis.Redis(connection)
            serialized = await self.serializer.serialize(header.dict())
            await redis.xadd(topic_name, {"data": serialized})
