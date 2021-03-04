import abc
from typing import Any, cast, Dict

# from uuid import UUID

import msgpack


class Serializer(abc.ABC):
    @abc.abstractmethod
    async def serialize(self, data: Dict[Any, Any]) -> bytes:
        raise NotImplementedError

    @abc.abstractmethod
    async def deserialize(self, data: bytes) -> Dict[Any, Any]:
        raise NotImplementedError


class MsgpackSerializer(Serializer):
    """
    @staticmethod
    def decode(obj: Any) -> Any:
        if b"__type__" in obj:
            if obj[b"__type__"] == b"uuid.UUID":
                obj = UUID(obj[b"__value__"].decode())
        return obj

    @staticmethod
    def encode(obj: Any) -> Any:
        if isinstance(obj, UUID):
            obj = {
                b"__type__": b"uuid.UUID",
                b"__value__": str(obj).encode(),
            }
        return obj
    """

    async def serialize(self, data: Dict[Any, Any]) -> bytes:
        # return cast(bytes, msgpack.packb(data, default=self.encode))
        return cast(bytes, msgpack.packb(data))

    async def deserialize(self, data: bytes) -> Dict[Any, Any]:
        # return cast(Dict[Any, Any], msgpack.unpackb(data, object_hook=self.decode))
        return cast(Dict[Any, Any], msgpack.unpackb(data))
