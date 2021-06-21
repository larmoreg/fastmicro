import logging
from typing import Generic, Type

from .serializer import Serializer, MsgpackSerializer
from .types import T

logger = logging.getLogger(__name__)


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
