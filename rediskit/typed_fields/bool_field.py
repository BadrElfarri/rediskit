from typing import Any, Literal

import strawberry
from pydantic import Field
from strawberry.experimental.pydantic import type as pyd_type

from rediskit.typed_fields.base_field import DataTypeFieldBase, FieldTypeEnum


class BoolField(DataTypeFieldBase):
    typename__: Literal[FieldTypeEnum.BoolField] = Field(default=FieldTypeEnum.BoolField, alias="__typename")
    default_bool: bool | None = Field(default=None, alias="defaultBool")

    def to_pydantic_field(self) -> tuple[str, tuple[type[bool], Any]]:
        field_kwargs: dict[str, Any] = {}

        if self.description:
            field_kwargs["description"] = self.description

        return self.label.lower(), (bool, Field(default=self.default_bool, **field_kwargs))

    def to_gql_type(self, extra: dict[str, Any] | None = None) -> "BoolFieldGql":
        obj = BoolFieldGql.from_pydantic(self, extra=extra)
        return obj


@pyd_type(model=BoolField)
class BoolFieldGql:
    id: strawberry.auto
    label: strawberry.auto
    description: strawberry.auto
    default_bool: strawberry.auto
