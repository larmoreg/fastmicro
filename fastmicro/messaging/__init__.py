import abc
import asyncio
from contextlib import asynccontextmanager
import logging
from pydantic import BaseModel
from pydantic.generics import GenericModel
from types import TracebackType
from typing import (
    Any,
    AsyncIterator,
    Generic,
    Optional,
    Sequence,
    Type,
    TypeVar,
)
from uuid import UUID

from fastmicro.env import (
    BATCH_SIZE,
    MESSAGING_TIMEOUT,
)
from fastmicro.serializer import SerializerABC
from fastmicro.serializer.json import Serializer

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class HeaderABC(GenericModel, Generic[T]):
    correlation_id: Optional[UUID] = None
    resends: int = 0
    message: Optional[T] = None
    error: Optional[str] = None


class MessagingABC(abc.ABC):
    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        if loop:
            self.loop = loop
        else:
            self.loop = asyncio.get_event_loop()

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


class TopicABC(abc.ABC, Generic[T]):
    @abc.abstractmethod
    def header(self, **kwargs: Any) -> HeaderABC[T]:
        raise NotImplementedError

    def __init__(
        self,
        name: str,
        messaging: MessagingABC,
        serializer_type: Type[SerializerABC] = Serializer,
    ):
        self.name = name
        self.messaging = messaging
        self.serializer_type = serializer_type

    async def serialize(self, header: HeaderABC[T]) -> bytes:
        return await self.serializer_type.serialize(header.dict())

    async def deserialize(self, serialized: bytes) -> HeaderABC[T]:
        data = await self.serializer_type.deserialize(serialized)
        return self.header(**data)

    async def subscribe(self, group_name: str) -> None:
        pass

    @asynccontextmanager
    async def _receive(
        self,
        group_name: str,
        consumer_name: str,
        timeout: Optional[float] = MESSAGING_TIMEOUT,
    ) -> AsyncIterator[HeaderABC[T]]:
        async with self._receive_batch(
            group_name, consumer_name, batch_size=1, timeout=timeout
        ) as headers:
            yield headers[0]

    @asynccontextmanager
    @abc.abstractmethod
    async def _receive_batch(
        self,
        group_name: str,
        consumer_name: str,
        batch_size: int = BATCH_SIZE,
        timeout: Optional[float] = MESSAGING_TIMEOUT,
    ) -> AsyncIterator[Sequence[HeaderABC[T]]]:
        raise NotImplementedError
        yield Sequence[HeaderABC[T]]

    @abc.abstractmethod
    async def _ack(self, group_name: str, header: HeaderABC[T]) -> None:
        raise NotImplementedError

    async def _ack_batch(
        self, group_name: str, headers: Sequence[HeaderABC[T]]
    ) -> None:
        tasks = [self._ack(group_name, header) for header in headers]
        await asyncio.gather(*tasks)

    @abc.abstractmethod
    async def _nack(self, group_name: str, header: HeaderABC[T]) -> None:
        raise NotImplementedError

    async def _nack_batch(
        self, group_name: str, headers: Sequence[HeaderABC[T]]
    ) -> None:
        tasks = [self._nack(group_name, header) for header in headers]
        await asyncio.gather(*tasks)

    @abc.abstractmethod
    async def _send(self, header: HeaderABC[T]) -> None:
        raise NotImplementedError

    async def _send_batch(self, headers: Sequence[HeaderABC[T]]) -> None:
        tasks = [self._send(header) for header in headers]
        await asyncio.gather(*tasks)

    @asynccontextmanager
    async def receive(
        self,
        group_name: str,
        consumer_name: str,
        timeout: Optional[float] = MESSAGING_TIMEOUT,
    ) -> AsyncIterator[HeaderABC[T]]:
        await self.subscribe(group_name)
        async with self._receive(group_name, consumer_name, timeout) as header:
            logger.debug(f"Received {header}")

            try:
                yield header

                logger.debug(f"Acking {header}")
                await self._ack(group_name, header)
            except Exception as e:
                logger.debug(f"Nacking {header}")
                await self._nack(group_name, header)
                raise e

    @asynccontextmanager
    async def receive_batch(
        self,
        group_name: str,
        consumer_name: str,
        batch_size: int = BATCH_SIZE,
        timeout: Optional[float] = MESSAGING_TIMEOUT,
    ) -> AsyncIterator[Sequence[HeaderABC[T]]]:
        await self.subscribe(group_name)
        async with self._receive_batch(
            group_name, consumer_name, batch_size, timeout
        ) as headers:
            if logger.isEnabledFor(logging.DEBUG):
                for header in headers:
                    logger.debug(f"Received {header}")

            try:
                yield headers

                if logger.isEnabledFor(logging.DEBUG):
                    for header in headers:
                        logger.debug(f"Acking {header}")
                await self._ack_batch(group_name, headers)
            except Exception as e:
                if headers:
                    if logger.isEnabledFor(logging.DEBUG):
                        for header in headers:
                            logger.debug(f"Nacking {header}")
                    await self._nack_batch(group_name, headers)
                raise e

    async def send(self, header: HeaderABC[T]) -> None:
        logger.debug(f"Sending {header}")
        await self._send(header)

    async def send_batch(self, headers: Sequence[HeaderABC[T]]) -> None:
        if logger.isEnabledFor(logging.DEBUG):
            for header in headers:
                logger.debug(f"Sending {header}")
        await self._send_batch(headers)
