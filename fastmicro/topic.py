from typing import Any, Generic, Optional, Type, TypeVar
from uuid import UUID, uuid4

import pydantic

from .serializer import Serializer, MsgpackSerializer

T = TypeVar("T", bound=pydantic.BaseModel)


class Header(pydantic.BaseModel):
    id: Optional[bytes]
    data: Optional[bytes]

    uuid: Optional[UUID]
    parent: Optional[UUID]
    message: Optional[Any]


class Topic(Generic[T]):
    def __init__(
        self,
        name: str,
        schema: Type[T],
        serializer: Type[Serializer] = MsgpackSerializer,
    ):
        self.name = name
        self.schema = schema
        self.serializer = serializer()

    async def serialize(self, header: Header) -> bytes:
        header.uuid = uuid4()
        assert header.message
        header.data = await self.serializer.serialize(header.message.dict())
        serialized = await self.serializer.serialize(header.dict())
        return serialized

    async def deserialize(self, message_id: bytes, serialized: bytes) -> Header:
        header_data = await self.serializer.deserialize(serialized)
        header = Header(**header_data)
        header.id = message_id
        assert header.data
        message_data = await self.serializer.deserialize(header.data)
        header.message = self.schema(**message_data)
        return header
