import json
from typing import Any, cast, Dict

from fastmicro.serializer import SerializerABC


class Serializer(SerializerABC):
    @classmethod
    async def serialize(cls, data: Dict[Any, Any]) -> bytes:
        return json.dumps(data, default=cls.encode).encode()

    @classmethod
    async def deserialize(cls, data: bytes) -> Dict[Any, Any]:
        return cast(
            Dict[Any, Any],
            json.loads(data.decode(), object_hook=cls.decode),
        )
