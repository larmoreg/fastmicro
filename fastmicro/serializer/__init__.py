import abc
from typing import Any, Dict
from uuid import UUID


class SerializerABC(abc.ABC):
    @staticmethod
    def decode(obj: Any) -> Any:
        if "__type__" in obj:
            if obj["__type__"] == "uuid.UUID":
                obj = UUID(obj["__value__"])
        return obj

    @staticmethod
    def encode(obj: Any) -> Any:
        if isinstance(obj, UUID):
            obj = {"__type__": "uuid.UUID", "__value__": str(obj)}
        return obj

    @classmethod
    @abc.abstractmethod
    async def serialize(cls, data: Dict[Any, Any]) -> bytes:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    async def deserialize(cls, data: bytes) -> Dict[Any, Any]:
        raise NotImplementedError
