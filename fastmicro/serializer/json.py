import json
from pydantic.json import pydantic_encoder
from typing import Any, cast, Dict

from fastmicro.serializer import SerializerABC


class Serializer(SerializerABC):
    @staticmethod
    async def serialize(data: Dict[Any, Any]) -> bytes:
        return json.dumps(data, default=pydantic_encoder).encode()

    @staticmethod
    async def deserialize(data: bytes) -> Dict[Any, Any]:
        return cast(Dict[Any, Any], json.loads(data.decode()))
