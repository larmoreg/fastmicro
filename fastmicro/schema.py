from pydantic import BaseModel
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from pydantic.typing import AbstractSetIntStr, DictStrAny, MappingIntStrAny


class CustomBaseModel(BaseModel):
    def dict(
        self,
        *,
        exclude_hidden: bool = True,
        exclude: Union["AbstractSetIntStr", "MappingIntStrAny"] = None,
        **kwargs,
    ) -> "DictStrAny":
        if exclude_hidden:
            temp: AbstractSetIntStr = {
                name
                for name, field in self.__fields__.items()
                if field.field_info.extra.get("hidden")
            }
            if exclude:
                temp |= set(exclude)
            exclude = temp
        return super().dict(exclude=exclude, **kwargs)
