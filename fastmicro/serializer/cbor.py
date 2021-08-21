import cbor2
from cbor2.decoder import CBORDecoder
from typing import Any, cast, Dict

from fastmicro.serializer import SerializerABC


class Serializer(SerializerABC):
    @classmethod
    def decode(cls, decoder: CBORDecoder, obj: Any) -> Any:  # type: ignore
        return super().decode(obj)

    @classmethod
    async def serialize(cls, data: Dict[Any, Any]) -> bytes:
        return cast(bytes, cbor2.dumps(data, default=cls.encode))

    @classmethod
    async def deserialize(cls, data: bytes) -> Dict[Any, Any]:
        return cast(Dict[Any, Any], cbor2.loads(data, object_hook=cls.decode))
