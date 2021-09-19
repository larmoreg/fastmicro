import abc
import asyncio
from contextlib import asynccontextmanager
import logging
from pydantic import BaseModel
from typing import AsyncIterator, List, Optional, Tuple, Type, TypeVar
from uuid import UUID

from fastmicro.env import (
    BATCH_SIZE,
    MESSAGING_TIMEOUT,
)
from fastmicro.topic import T, Topic

logger = logging.getLogger(__name__)


class HeaderABC(abc.ABC, BaseModel):
    correlation_id: Optional[UUID] = None
    resends: int = 0
    data: Optional[bytes] = None


HT = TypeVar("HT", bound=HeaderABC)


class MessagingABC(abc.ABC):
    @property
    def header_type(self) -> Type[HT]:
        raise NotImplementedError

    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        pass

    async def connect(self) -> None:
        pass

    async def cleanup(self) -> None:
        pass

    async def subscribe(self, topic_name: str, group_name: str) -> None:
        pass

    @abc.abstractmethod
    async def _receive(
        self, topic: Topic[T], group_name: str, consumer_name: str
    ) -> Tuple[HT, T]:
        raise NotImplementedError

    async def _receive_batch(
        self,
        topic: Topic[T],
        group_name: str,
        consumer_name: str,
        batch_size: int = BATCH_SIZE,
        timeout: Optional[float] = MESSAGING_TIMEOUT,
    ) -> Tuple[List[HT], List[T]]:
        tasks = [
            self._receive(topic, group_name, consumer_name) for i in range(batch_size)
        ]
        temp = await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout)
        headers, messages = zip(*temp)
        return headers, messages

    @abc.abstractmethod
    async def _ack(self, topic_name: str, group_name: str, header: HT) -> None:
        raise NotImplementedError

    async def _ack_batch(
        self, topic_name: str, group_name: str, headers: List[HT]
    ) -> None:
        tasks = [self._ack(topic_name, group_name, header) for header in headers]
        await asyncio.gather(*tasks)

    @abc.abstractmethod
    async def _nack(self, topic_name: str, group_name: str, header: HT) -> None:
        raise NotImplementedError

    async def _nack_batch(
        self, topic_name: str, group_name: str, headers: List[HT]
    ) -> None:
        tasks = [self._nack(topic_name, group_name, header) for header in headers]
        await asyncio.gather(*tasks)

    @abc.abstractmethod
    async def _send(self, topic: Topic[T], header: HT, message: T) -> None:
        raise NotImplementedError

    async def _send_batch(
        self, topic: Topic[T], headers: List[HT], messages: List[T]
    ) -> None:
        tasks = [
            self._send(topic, header, message)
            for header, message in zip(headers, messages)
        ]
        await asyncio.gather(*tasks)

    @asynccontextmanager
    async def receive(
        self, topic: Topic[T], group_name: str, consumer_name: str
    ) -> AsyncIterator[Tuple[HT, T]]:
        try:
            await self.subscribe(topic.name, group_name)
            header, message = await self._receive(topic, group_name, consumer_name)
            logger.debug(f"Received {header}: {message}")

            yield header, message

            logger.debug(f"Acking {header}")
            await self._ack(topic.name, group_name, header)
        except Exception as e:
            logger.debug(f"Nacking {header}")
            await self._nack(topic.name, group_name, header)
            raise e

    @asynccontextmanager
    async def receive_batch(
        self,
        topic: Topic[T],
        group_name: str,
        consumer_name: str,
        batch_size: int = BATCH_SIZE,
        timeout: Optional[float] = MESSAGING_TIMEOUT,
    ) -> AsyncIterator[Tuple[List[HT], List[T]]]:
        headers = list()
        try:
            await self.subscribe(topic.name, group_name)
            headers, messages = await self._receive_batch(
                topic, group_name, consumer_name, batch_size, timeout
            )
            if headers:
                if logger.isEnabledFor(logging.DEBUG):
                    for header, message in zip(headers, messages):
                        logger.debug(f"Received {header}: {message}")

                yield headers, messages

                if logger.isEnabledFor(logging.DEBUG):
                    for header in headers:
                        logger.debug(f"Acking {header}")
                await self._ack_batch(topic.name, group_name, headers)
            else:
                yield headers, messages
        except Exception as e:
            if headers:
                if logger.isEnabledFor(logging.DEBUG):
                    for header in headers:
                        logger.debug(f"Nacking {header}")
                await self._nack_batch(topic.name, group_name, headers)
            raise e

    async def send(self, topic: Topic[T], header: HT, message: T) -> None:
        logger.debug(f"Sending {header}: {message}")
        await self._send(topic, header, message)

    async def send_batch(
        self, topic: Topic[T], headers: List[HT], messages: List[T]
    ) -> None:
        if logger.isEnabledFor(logging.DEBUG):
            for header, message in zip(headers, messages):
                logger.debug(f"Sending {header}: {message}")
        await self._send_batch(topic, headers, messages)
