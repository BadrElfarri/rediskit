from enum import Enum as PyEnum
from typing import Any, Literal

import strawberry
from pydantic import Field
from strawberry.experimental.pydantic import type as pyd_type

from rediskit.typed_fields.base_field import DataTypeFieldBase


class EnumField(DataTypeFieldBase):
    typename__: Literal["EnumField"] = Field(default="EnumField", alias="__typename")
    allowed_values: list[str] | None = Field(default=None, alias="allowedValues")
    default_str: str | None = Field(default=None, alias="defaultStr")

    def to_pydantic_field(self) -> tuple[str, tuple[type[PyEnum], Any]]:
        field_kwargs: dict[str, Any] = {}

        if self.description:
            field_kwargs["description"] = self.description

        enum_name: str = f"{self.label.capitalize()}Enum"
        enum_class = PyEnum(enum_name, {val.upper(): val for val in self.allowed_values})  # type: ignore # mypy complaining about passed in variables
        return self.label.lower(), (enum_class, Field(default=self.default_str, **field_kwargs))

    def to_gql_type(self, extra: dict[str, Any] | None = None) -> "EnumFieldGql":
        obj = EnumFieldGql.from_pydantic(self, extra=extra)
        return obj


@pyd_type(model=EnumField)
class EnumFieldGql:
    id: strawberry.auto
    label: strawberry.auto
    description: strawberry.auto
    default_str: strawberry.auto
    allowed_values: strawberry.auto
