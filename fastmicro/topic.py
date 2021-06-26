import logging
import pydantic
from typing import Generic, Type, TypeVar

from .serializer import Serializer, MsgpackSerializer

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=pydantic.BaseModel)


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

    async def serialize(self, message: T) -> bytes:
        return await self.serializer.serialize(message.dict())

    async def deserialize(self, serialized: bytes) -> T:
        data = await self.serializer.deserialize(serialized)
        return self.schema(**data)
