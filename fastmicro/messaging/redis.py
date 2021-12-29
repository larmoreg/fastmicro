import aioredis
import asyncio
import sys
from typing import (
    cast,
    Dict,
    Generic,
    Optional,
    Sequence,
    Type,
)

from fastmicro.env import (
    BATCH_SIZE,
    MESSAGING_TIMEOUT,
    REDIS_ADDRESS,
)
from fastmicro.messaging import MessagingABC
from fastmicro.messaging.header import T, HeaderABC
from fastmicro.serializer import SerializerABC
from fastmicro.serializer.json import Serializer


class Header(HeaderABC[T], Generic[T]):
    message_id: Optional[bytes] = None


class Messaging(MessagingABC):
    def header_type(self, schema_type: Type[T]) -> Type[Header[T]]:
        return Header[schema_type]  # type: ignore

    def __init__(
        self,
        address: str = REDIS_ADDRESS,
        serializer_type: Type[SerializerABC] = Serializer,
        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop(),
    ) -> None:
        super().__init__(serializer_type, loop)
        self.address = address
        self.initialized = False

    async def _topic_exists(self, topic_name: str) -> bool:
        try:
            await self.redis.xinfo_stream(topic_name)
        except aioredis.exceptions.ResponseError:
            return False
        return True

    async def _group_exists(self, topic_name: str, group_name: str) -> bool:
        group_infos = await self.redis.xinfo_groups(topic_name)
        if any(group_info["name"] == group_name.encode() for group_info in group_infos):
            return True
        return False

    async def _create_group(self, topic_name: str, group_name: str) -> None:
        if not await self._topic_exists(topic_name) or not await self._group_exists(
            topic_name, group_name
        ):
            await self.redis.xgroup_create(
                topic_name, group_name, id="$", mkstream=True
            )

    async def connect(self) -> None:
        if not self.initialized:
            self.redis = await aioredis.from_url(self.address)  # type: ignore

    async def cleanup(self) -> None:
        if self.initialized:
            await self.redis.close()

    async def subscribe(self, topic_name: str, group_name: str) -> None:
        await self._create_group(topic_name, group_name)

    async def ack(
        self, topic_name: str, group_name: str, headers: Sequence[HeaderABC[T]]
    ) -> None:
        headers = cast(Sequence[Header[T]], headers)
        message_ids = [header.message_id for header in headers]
        await self.redis.xack(topic_name, group_name, *message_ids)

    async def nack(
        self, topic_name: str, group_name: str, headers: Sequence[HeaderABC[T]]
    ) -> None:
        pass

    async def _receive(
        self, message_id: bytes, raw_message: Dict[bytes, bytes], schema_type: Type[T]
    ) -> Header[T]:
        header = cast(
            Header[T], await self.deserialize(raw_message[b"data"], schema_type)
        )
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
        temp = await self.redis.xreadgroup(
            group_name,
            consumer_name,
            {topic_name: ">"},
            count=batch_size,
            block=sys.maxsize
            if timeout is None
            else int(timeout * 1000)
            if timeout > 0
            else None,
        )
        if not temp:
            raise asyncio.TimeoutError(f"Timed out after {timeout} sec")

        tasks = [
            self._receive(message_id, message, schema_type)
            for message_id, message in temp[0][1]
        ]
        headers = await asyncio.gather(*tasks)
        return cast(Sequence[Header[T]], headers)

    async def _send(self, topic_name: str, header: Header[T]) -> None:
        serialized = await self.serialize(header)
        await self.redis.xadd(topic_name, {"data": serialized})

    async def send(
        self,
        topic_name: str,
        headers: Sequence[HeaderABC[T]],
    ) -> None:
        headers = cast(Sequence[Header[T]], headers)
        tasks = [self._send(topic_name, header) for header in headers]
        await asyncio.gather(*tasks)
