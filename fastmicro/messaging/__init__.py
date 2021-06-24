import abc
import asyncio
from contextlib import asynccontextmanager
import logging
from typing import (
    AsyncIterator,
    Generic,
    List,
    Sequence,
    Type,
    Union,
)
from uuid import uuid4

from fastmicro.env import (
    BATCH_SIZE,
    TIMEOUT,
)
from fastmicro.serializer import Serializer, MsgpackSerializer
from fastmicro.topic import Topic
from fastmicro.types import T, HT

logger = logging.getLogger(__name__)


class Messaging(Generic[HT], abc.ABC):
    @property
    def header_type(self) -> Type[HT]:
        raise NotImplementedError

    def __init__(self, serializer: Type[Serializer] = MsgpackSerializer):
        self.serializer = serializer()

    async def connect(self) -> None:
        pass

    async def cleanup(self) -> None:
        pass

    async def _subscribe(self, topic_name: str, group_name: str) -> None:
        pass

    @abc.abstractmethod
    async def _receive(self, topic_name: str, group_name: str, consumer_name: str) -> HT:
        raise NotImplementedError

    async def _receive_batch(
        self,
        topic_name: str,
        group_name: str,
        consumer_name: str,
        batch_size: int = BATCH_SIZE,
        timeout: float = TIMEOUT,
    ) -> List[HT]:
        tasks = [self._receive(topic_name, group_name, consumer_name) for i in range(batch_size)]
        return await asyncio.gather(*tasks)

    @abc.abstractmethod
    async def _ack(self, topic_name: str, group_name: str, header: HT) -> None:
        raise NotImplementedError

    async def _ack_batch(self, topic_name: str, group_name: str, headers: List[HT]) -> None:
        tasks = [self._ack(topic_name, group_name, header) for header in headers]
        await asyncio.gather(*tasks)

    @abc.abstractmethod
    async def _nack(self, topic_name: str, group_name: str, header: HT) -> None:
        raise NotImplementedError

    async def _nack_batch(self, topic_name: str, group_name: str, headers: List[HT]) -> None:
        tasks = [self._nack(topic_name, group_name, header) for header in headers]
        await asyncio.gather(*tasks)

    @abc.abstractmethod
    async def _send(self, topic_name: str, header: HT) -> None:
        raise NotImplementedError

    async def _send_batch(self, topic_name: str, headers: List[HT]) -> None:
        tasks = [self._send(topic_name, header) for header in headers]
        await asyncio.gather(*tasks)

    @asynccontextmanager
    async def transaction(self, topic_name: str) -> AsyncIterator:
        yield

    @asynccontextmanager
    async def receive(
        self, topic: Topic[T], group_name: str, consumer_name: str
    ) -> AsyncIterator[HT]:
        try:
            await self._subscribe(topic.name, group_name)
            header = await self._receive(topic.name, group_name, consumer_name)
            logger.debug(f"Received {header.uuid}")
            header.message = await topic.deserialize(header.data)  # type: ignore
            header.data = None

            yield header

            logger.debug(f"Acking {header.uuid}")
            await self._ack(topic.name, group_name, header)
        except Exception as e:
            logger.debug(f"Nacking {header.uuid}")
            await self._nack(topic.name, group_name, header)
            raise e

    @asynccontextmanager
    async def receive_batch(
        self,
        topic: Topic[T],
        group_name: str,
        consumer_name: str,
        batch_size: int = BATCH_SIZE,
        timeout: float = TIMEOUT,
    ) -> AsyncIterator[List[HT]]:
        try:
            await self._subscribe(topic.name, group_name)
            headers = await self._receive_batch(
                topic.name, group_name, consumer_name, batch_size, timeout
            )
            if headers:
                for header in headers:
                    logger.debug(f"Received {header.uuid}")
                    header.message = await topic.deserialize(header.data)  # type: ignore
                    header.data = None

                yield headers

                for header in headers:
                    logger.debug(f"Acking {header.uuid}")
                await self._ack_batch(topic.name, group_name, headers)
            else:
                yield headers
        except Exception as e:
            if headers:
                for header in headers:
                    logger.debug(f"Nacking {header.uuid}")
                await self._nack_batch(topic.name, group_name, headers)
            raise e

    async def send(self, topic: Topic[T], message: Union[HT, T]) -> HT:
        if isinstance(message, self.header_type):
            header = message
        else:
            header = self.header_type(message=message)

        header.uuid = uuid4()
        assert header.message
        header.data = await topic.serialize(header.message)
        header.message = None

        logger.debug(f"Sending {header.uuid}")
        await self._send(topic.name, header)
        return header

    async def send_batch(self, topic: Topic[T], messages: Sequence[Union[HT, T]]) -> List[HT]:
        headers: List[HT] = list()
        for message in messages:
            if isinstance(message, self.header_type):
                header = message
            else:
                header = self.header_type(message=message)

            header.uuid = uuid4()
            assert header.message
            header.data = await topic.serialize(header.message)
            header.message = None

            headers.append(header)
            logger.debug(f"Sending {header.uuid}")

        await self._send_batch(topic.name, headers)
        return headers
