from pydantic.generics import GenericModel
from typing import (
    Any,
    Generic,
    Optional,
    TypeVar,
)
from uuid import UUID

T = TypeVar("T", bound=Any)


class HeaderABC(GenericModel, Generic[T]):
    correlation_id: Optional[UUID] = None
    resends: int = 0
    message: Optional[T] = None
    error: Optional[str] = None
