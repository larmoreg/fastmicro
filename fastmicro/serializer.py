import abc
import json
from typing import Any, cast, Dict


class Serializer(abc.ABC):
    @abc.abstractmethod
    async def serialize(self, data: Dict[Any, Any]) -> bytes:
        raise NotImplementedError

    @abc.abstractmethod
    async def deserialize(self, data: bytes) -> Dict[Any, Any]:
        raise NotImplementedError


class JsonSerializer(Serializer):
    async def serialize(self, data: Dict[Any, Any]) -> bytes:
        return json.dumps(data).encode()

    async def deserialize(self, data: bytes) -> Dict[Any, Any]:
        return cast(Dict[Any, Any], json.loads(data.decode()))
