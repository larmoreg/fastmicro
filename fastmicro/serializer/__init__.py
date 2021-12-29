import abc
from pydantic import BaseModel
from typing import Any, Dict, Union

from fastmicro.messaging.header import T, HeaderABC


class SerializerABC(abc.ABC):
    @staticmethod
    @abc.abstractmethod
    async def serialize(data: Union[BaseModel, HeaderABC[T]]) -> bytes:
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    async def deserialize(data: bytes) -> Dict[Any, Any]:
        raise NotImplementedError
