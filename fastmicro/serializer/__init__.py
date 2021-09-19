import abc
from datetime import datetime
from typing import Any, Dict
from uuid import UUID


class SerializerABC(abc.ABC):
    @staticmethod
    def decode(obj: Any) -> Any:
        if "__type__" in obj:
            if obj["__type__"] == "uuid.UUID":
                obj = UUID(obj["__value__"])
            elif obj["__type__"] == "datetime.datetime":
                obj = datetime.fromisoformat(obj["__value__"])
            elif obj["__type__"] == "bytes":
                obj = bytes.fromhex(obj["__value__"])
            else:
                raise ValueError(f'Unknown type: {obj["__type__"]}')
        return obj

    @staticmethod
    def encode(obj: Any) -> Any:
        if isinstance(obj, UUID):
            obj = {"__type__": "uuid.UUID", "__value__": str(obj)}
        elif isinstance(obj, datetime):
            obj = {"__type__": "datetime.datetime", "__value__": obj.isoformat()}
        elif isinstance(obj, bytes):
            obj = {"__type__": "bytes", "__value__": obj.hex()}
        else:
            raise ValueError(f"Unknown type: {type(obj)}")
        return obj

    @classmethod
    @abc.abstractmethod
    async def serialize(cls, data: Dict[Any, Any]) -> bytes:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    async def deserialize(cls, data: bytes) -> Dict[Any, Any]:
        raise NotImplementedError
