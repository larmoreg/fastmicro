import abc
from pydantic import BaseModel
from typing import Any, Dict


class SerializerABC(abc.ABC):
    @staticmethod
    @abc.abstractmethod
    async def serialize(data: BaseModel) -> bytes:
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    async def deserialize(data: bytes) -> Dict[Any, Any]:
        raise NotImplementedError
