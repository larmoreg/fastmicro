from functools import partial
import msgpack
from pydantic import BaseModel
from pydantic.json import custom_pydantic_encoder, pydantic_encoder
from typing import Any, cast, Dict

from fastmicro.serializer import SerializerABC


class Serializer(SerializerABC):
    @staticmethod
    async def serialize(data: BaseModel) -> bytes:
        if data.__config__.json_encoders:
            encoder = partial(custom_pydantic_encoder, data.__config__.json_encoders)
            return cast(bytes, msgpack.dumps(data.dict(), default=encoder))
        else:
            return cast(bytes, msgpack.dumps(data.dict(), default=pydantic_encoder))

    @staticmethod
    async def deserialize(data: bytes) -> Dict[Any, Any]:
        return cast(Dict[Any, Any], msgpack.loads(data))
