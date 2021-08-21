import logging
from typing import Generic, Type, TypeVar

from fastmicro.schema import CustomBaseModel
from fastmicro.serializer import SerializerABC
from fastmicro.serializer.json import Serializer

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=CustomBaseModel)


class Topic(Generic[T]):
    def __init__(
        self,
        name: str,
        schema: Type[T],
        serializer: Type[SerializerABC] = Serializer,
    ):
        self.name = name
        self.schema = schema
        self.serializer = serializer

    async def serialize(self, message: T) -> bytes:
        return await self.serializer.serialize(message.dict(exclude_hidden=False))

    async def deserialize(self, serialized: bytes) -> T:
        data = await self.serializer.deserialize(serialized)
        return self.schema(**data)
