from contextlib import asynccontextmanager
import logging
from typing import Any, AsyncIterator, Generic, Type, TypeVar, Union

import pydantic

from .messaging import Header, Messaging
from .registrar import registrar

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=pydantic.BaseModel)


class Topic(Generic[T]):
    def __init__(
        self,
        messaging: Messaging,
        name: str,
        schema: Type[T],
        serializer: str = "msgpack",
    ):
        self.messaging = messaging
        self.name = name
        self.schema = schema
        self.serializer = registrar.get(serializer)

    async def serialize(self, message: T) -> bytes:
        return await self.serializer.serialize(message.dict())

    async def deserialize(self, serialized: bytes) -> T:
        message = await self.serializer.deserialize(serialized)
        return self.schema(**message)

    @asynccontextmanager
    async def receive(self, name: str) -> AsyncIterator[Header]:
        try:
            header = await self.messaging.receive(self.name, name)
            assert header.data
            header.message = await self.deserialize(header.data)
            yield header
            assert header.id
            await self.messaging.ack(self.name, name, header.id)
        except Exception:
            logger.exception("Processing failed; nacking message")
            assert header.id
            await self.messaging.nack(self.name, name, header.id)

    async def send(self, message: Union[Header, Any]) -> Header:
        if isinstance(message, Header):
            header = message
        else:
            header = Header(message=message)
        assert header.message
        header.data = await self.serialize(header.message)
        await self.messaging.send(self.name, header)
        return header
