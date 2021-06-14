from typing import Any, Generic, Optional, Type, TypeVar
from uuid import UUID

import pydantic

from .serializer import Serializer, MsgpackSerializer

T = TypeVar("T", bound=pydantic.BaseModel)


class Header(pydantic.BaseModel):
    uuid: Optional[UUID]
    parent: Optional[UUID]
    data: Optional[bytes]
    message: Optional[Any]


class Topic(Generic[T]):
    def __init__(
        self, name: str, schema: Type[T], serializer: Type[Serializer] = MsgpackSerializer
    ):
        self.name = name
        self.schema = schema
        self.serializer = serializer()

    async def serialize(self, message: Any) -> bytes:
        return await self.serializer.serialize(message.dict())

    async def deserialize(self, serialized: bytes) -> T:
        data = await self.serializer.deserialize(serialized)
        return self.schema(**data)
