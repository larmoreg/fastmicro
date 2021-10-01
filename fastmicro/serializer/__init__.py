import abc
from typing import Any, Dict


class SerializerABC(abc.ABC):
    @staticmethod
    @abc.abstractmethod
    async def serialize(data: Dict[Any, Any]) -> bytes:
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    async def deserialize(data: bytes) -> Dict[Any, Any]:
        raise NotImplementedError
