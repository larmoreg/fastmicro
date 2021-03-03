from contextlib import asynccontextmanager
from typing import AsyncIterator, Generic, Type, TypeVar

import pydantic

from .messaging import Header, Messaging
from .registrar import registrar

T = TypeVar("T", bound=pydantic.BaseModel)


class Topic(Generic[T]):
    def __init__(self, messaging: Messaging, name: str, schema: Type[T], serializer: str = "json"):
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
        header = await self.messaging.receive(self.name, name)
        try:
            assert header.data
            header.message = await self.deserialize(header.data)
            yield header
            await self.messaging.ack(self.name, name, header)
        except Exception:
            await self.messaging.nack(self.name, name, header)

    async def send(self, header: Header) -> None:
        assert header.message
        header.data = await self.serialize(header.message)
        await self.messaging.send(self.name, header)
