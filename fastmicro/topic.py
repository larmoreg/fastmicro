import logging
from pydantic import BaseModel
from typing import Generic, Type, TypeVar

from fastmicro.serializer import SerializerABC
from fastmicro.serializer.json import Serializer

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class Topic(Generic[T]):
    def __init__(
        self,
        name: str,
        schema_type: Type[T],
        serializer_type: Type[SerializerABC] = Serializer,
    ):
        self.name = name
        self.schema_type = schema_type
        self.serializer_type = serializer_type

    async def serialize(self, message: T) -> bytes:
        return await self.serializer_type.serialize(message.dict())

    async def deserialize(self, serialized: bytes) -> T:
        data = await self.serializer_type.deserialize(serialized)
        return self.schema_type(**data)
