import msgpack
from typing import Any, cast, Dict

from fastmicro.serializer import SerializerABC


class Serializer(SerializerABC):
    @classmethod
    async def serialize(cls, data: Dict[Any, Any]) -> bytes:
        return cast(bytes, msgpack.packb(data, default=cls.encode))

    @classmethod
    async def deserialize(cls, data: bytes) -> Dict[Any, Any]:
        return cast(Dict[Any, Any], msgpack.unpackb(data, object_hook=cls.decode))
