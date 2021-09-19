import aioredis
import asyncio
import logging
import sys
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
)

from fastmicro.env import (
    BATCH_SIZE,
    MESSAGING_TIMEOUT,
    REDIS_ADDRESS,
)
from fastmicro.messaging import HeaderABC, HT, MessagingABC
from fastmicro.topic import T, Topic

logger = logging.getLogger(__name__)


class Header(HeaderABC):
    message_id: Optional[bytes] = None


header_topic = Topic("", Header)


class Messaging(MessagingABC):
    header_type: Type[HT] = Header

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

    async def connect(self) -> None:
        self.redis = await aioredis.create_redis_pool(
            self.address, loop=self.loop, minsize=3
        )

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
        if any(
            group_info[b"name"] == group_name.encode() for group_info in group_infos
        ):
            return True
        return False

    async def _create_group(self, topic_name: str, group_name: str) -> None:
        if not await self._topic_exists(topic_name) or not await self._group_exists(
            topic_name, group_name
        ):
            assert self.redis
            await self.redis.xgroup_create(
                topic_name, group_name, latest_id="$", mkstream=True
            )

    async def subscribe(self, topic_name: str, group_name: str) -> None:
        await self._create_group(topic_name, group_name)

    @staticmethod
    async def _raw_receive(
        topic: Topic[T], temp_message: Dict[bytes, bytes], message_id: bytes
    ) -> Tuple[Header, T]:
        header = await header_topic.deserialize(temp_message[b"data"])
        header.message_id = message_id

        assert header.data
        message = await topic.deserialize(header.data)
        return header, message

    async def _receive(
        self, topic: Topic[T], group_name: str, consumer_name: str
    ) -> Tuple[Header, T]:
        headers, messages = await self._receive_batch(
            topic, group_name, consumer_name, batch_size=1, timeout=0
        )
        return headers[0], messages[0]

    async def _receive_batch(
        self,
        topic: Topic[T],
        group_name: str,
        consumer_name: str,
        batch_size: int = BATCH_SIZE,
        timeout: Optional[float] = MESSAGING_TIMEOUT,
    ) -> Tuple[List[Header], List[T]]:
        assert self.redis
        temp = await self.redis.xread_group(
            group_name,
            consumer_name,
            [topic.name],
            timeout=int(timeout * 1000) if timeout else sys.maxsize,
            count=batch_size,
            latest_ids=[">"],
        )

        tasks = [
            self._raw_receive(topic, message, message_id)
            for stream, message_id, message in temp
        ]
        temp = await asyncio.gather(*tasks)
        headers, messages = zip(*temp)
        return headers, messages

    async def _ack(self, topic_name: str, group_name: str, header: Header) -> None:
        await self._ack_batch(topic_name, group_name, [header])

    async def _ack_batch(
        self, topic_name: str, group_name: str, headers: List[Header]
    ) -> None:
        assert self.redis
        message_ids = [header.message_id for header in headers]
        await self.redis.xack(topic_name, group_name, *message_ids)

    async def _nack(self, topic_name: str, group_name: str, header: Header) -> None:
        pass

    async def _nack_batch(
        self, topic_name: str, group_name: str, headers: List[Header]
    ) -> None:
        pass

    async def _raw_send(self, topic: Topic[T], header: Header, message: T) -> None:
        serialized = await topic.serialize(message)
        header.data = serialized

        serialized = await header_topic.serialize(header)
        await self.redis.xadd(topic.name, {"data": serialized})

    async def _send(self, topic: Topic[T], header: Header, message: T) -> None:
        await self._raw_send(topic, header, message)

    async def _send_batch(
        self, topic: Topic[T], headers: List[Header], messages: List[T]
    ) -> None:
        tasks = [
            self._raw_send(topic, header, message)
            for header, message in zip(headers, messages)
        ]
        await asyncio.gather(*tasks)
