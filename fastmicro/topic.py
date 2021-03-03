from contextlib import asynccontextmanager
from typing import AsyncIterator, Generic, Type, TypeVar

import pydantic

from .messaging import Messaging
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
    async def receive(self, name: str) -> AsyncIterator[T]:
        serialized = await self.messaging.receive(self.name, name)
        try:
            message = await self.deserialize(serialized)
            yield message
            await self.messaging.ack(self.name, name, serialized)
        except Exception:
            await self.messaging.nack(self.name, name, serialized)

    async def send(self, message: T) -> None:
        serialized = await self.serialize(message)
        await self.messaging.send(self.name, serialized)
