from enum import Enum as PyEnum
from typing import Any, Literal

from pydantic import Field

from rediskit.typed_fields.base_field import DataTypeFieldBase


class EnumField(DataTypeFieldBase):
    typename__: Literal["EnumField"] = Field(default="EnumField", alias="__typename")
    allowed_values: list[str]
    value: str | None = None

    def to_pydantic_field(self) -> tuple[str, tuple[type[PyEnum], Any]]:
        field_kwargs: dict[str, Any] = {}

        if self.description:
            field_kwargs["description"] = self.description

        enum_name = f"{self.label.capitalize()}Enum"
        enum_class = PyEnum(enum_name, {val.upper(): val for val in self.allowed_values})
        return self.label.lower(), (enum_class, Field(default=self.value, **field_kwargs))
