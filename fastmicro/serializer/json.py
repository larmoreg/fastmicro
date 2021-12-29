from functools import partial
import json
from pydantic import BaseModel
from pydantic.json import custom_pydantic_encoder, pydantic_encoder
from typing import Any, cast, Dict, Union

from fastmicro.messaging.header import T, HeaderABC
from fastmicro.serializer import SerializerABC


class Serializer(SerializerABC):
    @staticmethod
    async def serialize(data: Union[BaseModel, HeaderABC[T]]) -> bytes:
        if (
            isinstance(data, HeaderABC)
            and isinstance(data.message, BaseModel)
            and data.message.__config__.json_encoders
        ):
            encoder = partial(
                custom_pydantic_encoder, data.message.__config__.json_encoders
            )
            return json.dumps(data.dict(), default=encoder).encode()
        elif data.__config__.json_encoders:
            encoder = partial(custom_pydantic_encoder, data.__config__.json_encoders)
            return json.dumps(data.dict(), default=encoder).encode()
        else:
            return json.dumps(data.dict(), default=pydantic_encoder).encode()

    @staticmethod
    async def deserialize(data: bytes) -> Dict[Any, Any]:
        return cast(Dict[Any, Any], json.loads(data.decode()))
