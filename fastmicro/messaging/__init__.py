import abc
import asyncio
from contextlib import asynccontextmanager
import logging
import pydantic
from typing import AsyncIterator, Generic, List, Optional, TypeVar
from uuid import uuid4, UUID

from fastmicro.env import (
    BATCH_SIZE,
    TIMEOUT,
)
from fastmicro.topic import Topic

logger = logging.getLogger(__name__)


class MessageABC(abc.ABC, pydantic.BaseModel):
    uuid: Optional[UUID]
    parent: Optional[UUID]


T = TypeVar("T", bound=MessageABC)


class MessagingABC(Generic[T], abc.ABC):
    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        pass

    async def connect(self) -> None:
        pass

    async def cleanup(self) -> None:
        pass

    async def subscribe(self, topic_name: str, group_name: str) -> None:
        pass

    @abc.abstractmethod
    async def _receive(self, topic: Topic[T], group_name: str, consumer_name: str) -> T:
        raise NotImplementedError

    async def _receive_batch(
        self,
        topic: Topic[T],
        group_name: str,
        consumer_name: str,
        batch_size: int = BATCH_SIZE,
        timeout: float = TIMEOUT,
    ) -> List[T]:
        tasks = [self._receive(topic, group_name, consumer_name) for i in range(batch_size)]
        return await asyncio.gather(*tasks)

    @abc.abstractmethod
    async def _ack(self, topic_name: str, group_name: str, message: T) -> None:
        raise NotImplementedError

    async def _ack_batch(self, topic_name: str, group_name: str, messages: List[T]) -> None:
        tasks = [self._ack(topic_name, group_name, message) for message in messages]
        await asyncio.gather(*tasks)

    @abc.abstractmethod
    async def _nack(self, topic_name: str, group_name: str, message: T) -> None:
        raise NotImplementedError

    async def _nack_batch(self, topic_name: str, group_name: str, messages: List[T]) -> None:
        tasks = [self._nack(topic_name, group_name, message) for message in messages]
        await asyncio.gather(*tasks)

    @abc.abstractmethod
    async def _send(self, topic: Topic[T], message: T) -> None:
        raise NotImplementedError

    async def _send_batch(self, topic: Topic[T], messages: List[T]) -> None:
        tasks = [self._send(topic, message) for message in messages]
        await asyncio.gather(*tasks)

    @asynccontextmanager
    async def transaction(self, topic_name: str) -> AsyncIterator[None]:
        yield

    @asynccontextmanager
    async def receive(
        self, topic: Topic[T], group_name: str, consumer_name: str
    ) -> AsyncIterator[T]:
        try:
            await self.subscribe(topic.name, group_name)
            message = await self._receive(topic, group_name, consumer_name)
            logger.debug(f"Received {message.uuid}")

            yield message

            logger.debug(f"Acking {message.uuid}")
            await self._ack(topic.name, group_name, message)
        except Exception as e:
            logger.debug(f"Nacking {message.uuid}")
            await self._nack(topic.name, group_name, message)
            raise e

    @asynccontextmanager
    async def receive_batch(
        self,
        topic: Topic[T],
        group_name: str,
        consumer_name: str,
        batch_size: int = BATCH_SIZE,
        timeout: float = TIMEOUT,
    ) -> AsyncIterator[List[T]]:
        try:
            await self.subscribe(topic.name, group_name)
            messages = await self._receive_batch(
                topic, group_name, consumer_name, batch_size, timeout
            )
            if messages:
                for message in messages:
                    logger.debug(f"Received {message.uuid}")

                yield messages

                for message in messages:
                    logger.debug(f"Acking {message.uuid}")
                await self._ack_batch(topic.name, group_name, messages)
            else:
                yield messages
        except Exception as e:
            if messages:
                for message in messages:
                    logger.debug(f"Nacking {message.uuid}")
                await self._nack_batch(topic.name, group_name, messages)
            raise e

    async def send(self, topic: Topic[T], message: T) -> None:
        message.uuid = uuid4()
        logger.debug(f"Sending {message.uuid}")
        await self._send(topic, message)

    async def send_batch(self, topic: Topic[T], messages: List[T]) -> None:
        for message in messages:
            message.uuid = uuid4()
            logger.debug(f"Sending {message.uuid}")
        await self._send_batch(topic, messages)
