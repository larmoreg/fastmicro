import logging
from typing import Generic, Optional, Type, TypeVar
from uuid import UUID

import pydantic

from .serializer import Serializer, MsgpackSerializer

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=pydantic.BaseModel)


class Header(pydantic.BaseModel, Generic[T]):
    uuid: Optional[UUID]
    parent: Optional[UUID]
    data: Optional[bytes]
    message: Optional[T]


class Topic(Generic[T]):
    def __init__(
        self, name: str, schema: Type[T], serializer: Type[Serializer] = MsgpackSerializer
    ):
        self.name = name
        self.schema = schema
        self.serializer = serializer()

    async def serialize(self, message: T) -> bytes:
        return await self.serializer.serialize(message.dict())

    async def deserialize(self, serialized: bytes) -> T:
        data = await self.serializer.deserialize(serialized)
        return self.schema(**data)
