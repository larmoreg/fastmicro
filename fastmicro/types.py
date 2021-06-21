from typing import Generic, Optional, TypeVar
from uuid import UUID

import pydantic

T = TypeVar("T", bound=pydantic.BaseModel)


class Header(Generic[T], pydantic.BaseModel):
    uuid: Optional[UUID]
    parent: Optional[UUID]
    data: Optional[bytes]
    message: Optional[T]


HT = TypeVar("HT", bound=Header[T])  # type: ignore

QT = TypeVar("QT")

AT = TypeVar("AT", bound=pydantic.BaseModel)
BT = TypeVar("BT", bound=pydantic.BaseModel)
