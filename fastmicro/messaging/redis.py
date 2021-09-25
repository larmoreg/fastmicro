import aioredis
import asyncio
from contextlib import asynccontextmanager
import logging
import sys
from typing import (
    Any,
    AsyncIterator,
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
from fastmicro.messaging import T, HeaderABC, MessagingABC, TopicABC
from fastmicro.serializer import SerializerABC
from fastmicro.serializer.json import Serializer

logger = logging.getLogger(__name__)


class Header(HeaderABC[T], Generic[T]):
    message_id: Optional[bytes] = None


class Messaging(MessagingABC):
    def __init__(
        self,
        address: str = REDIS_ADDRESS,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        self.address = address
        super().__init__(loop)

    async def connect(self) -> None:
        self.redis = await aioredis.from_url(self.address)  # type: ignore

    async def cleanup(self) -> None:
        await self.redis.close()

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

    async def subscribe(self, group_name: str) -> None:
        await self.messaging._create_group(self.name, group_name)

    async def _raw_receive(
        self, temp_message: Dict[bytes, bytes], message_id: bytes
    ) -> Header[T]:
        header = await self.deserialize(temp_message[b"data"])
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
        temp = await self.messaging.redis.xreadgroup(
            group_name,
            consumer_name,
            {self.name: ">"},
            count=batch_size,
            block=sys.maxsize
            if timeout is None
            else int(timeout * 1000)
            if timeout > 0
            else None,
        )

        tasks = [
            self._raw_receive(message, message_id) for message_id, message in temp[0][1]
        ]
        if not tasks:
            raise asyncio.TimeoutError

        yield await asyncio.gather(*tasks)

    async def _ack(self, group_name: str, header: HeaderABC[T]) -> None:
        header = cast(Header[T], header)
        await self._ack_batch(group_name, [header])

    async def _ack_batch(
        self, group_name: str, headers: Sequence[HeaderABC[T]]
    ) -> None:
        headers = cast(Sequence[Header[T]], headers)
        message_ids = [header.message_id for header in headers]
        await self.messaging.redis.xack(self.name, group_name, *message_ids)

    async def _nack(self, group_name: str, header: HeaderABC[T]) -> None:
        pass

    async def _nack_batch(
        self, group_name: str, headers: Sequence[HeaderABC[T]]
    ) -> None:
        pass

    async def _raw_send(self, header: Header[T]) -> None:
        serialized = await self.serialize(header)
        await self.messaging.redis.xadd(self.name, {"data": serialized})

    async def _send(self, header: HeaderABC[T]) -> None:
        header = cast(Header[T], header)
        await self._raw_send(header)

    async def _send_batch(self, headers: Sequence[HeaderABC[T]]) -> None:
        headers = cast(Sequence[Header[T]], headers)
        tasks = [self._raw_send(header) for header in headers]
        await asyncio.gather(*tasks)
