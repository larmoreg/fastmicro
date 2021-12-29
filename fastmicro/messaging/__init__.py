import abc
import asyncio
from types import TracebackType
from typing import (
    Optional,
    Sequence,
    Type,
)

from fastmicro.env import BATCH_SIZE, MESSAGING_TIMEOUT
from fastmicro.messaging.header import T, HeaderABC
from fastmicro.messaging.topic import Topic
from fastmicro.serializer import SerializerABC
from fastmicro.serializer.json import Serializer


class MessagingABC(abc.ABC):
    @abc.abstractmethod
    def header_type(self, schema_type: Type[T]) -> Type[HeaderABC[T]]:
        raise NotImplementedError

    def __init__(
        self,
        serializer_type: Type[SerializerABC] = Serializer,
        loop: asyncio.AbstractEventLoop = asyncio.get_event_loop(),
    ):
        self.serializer_type = serializer_type
        self.loop = loop

    async def __aenter__(self) -> "MessagingABC":
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        await self.cleanup()

    async def connect(self) -> None:
        pass

    async def cleanup(self) -> None:
        pass

    async def serialize(self, header: HeaderABC[T]) -> bytes:
        return await self.serializer_type.serialize(header)

    async def deserialize(
        self, serialized: bytes, schema_type: Type[T]
    ) -> HeaderABC[T]:
        data = await self.serializer_type.deserialize(serialized)
        return self.header_type(schema_type)(**data)

    async def subscribe(self, topic_name: str, group_name: str) -> None:
        pass

    async def unsubscribe(self, topic_name: str, group_name: str) -> None:
        pass

    @abc.abstractmethod
    async def ack(
        self, topic_name: str, group_name: str, headers: Sequence[HeaderABC[T]]
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def nack(
        self, topic_name: str, group_name: str, headers: Sequence[HeaderABC[T]]
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def receive(
        self,
        topic_name: str,
        group_name: str,
        consumer_name: str,
        schema_type: Type[T],
        batch_size: int = BATCH_SIZE,
        timeout: Optional[float] = MESSAGING_TIMEOUT,
    ) -> Sequence[HeaderABC[T]]:
        raise NotImplementedError

    @abc.abstractmethod
    async def send(
        self,
        topic_name: str,
        headers: Sequence[HeaderABC[T]],
    ) -> None:
        raise NotImplementedError

    def topic(self, name: str, schema_type: Type[T]) -> Topic[T]:
        return Topic(self, name, schema_type)
