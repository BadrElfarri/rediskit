from typing import Any, Literal

import strawberry
from pydantic import Field
from strawberry.experimental.pydantic import type as pyd_type

from rediskit.typed_fields.base_field import DataTypeFieldBase, FieldTypeEnum


class StrField(DataTypeFieldBase):
    typename__: Literal[FieldTypeEnum.StrField] = Field(default=FieldTypeEnum.StrField, alias="__typename")
    min_length: int | None = None
    max_length: int | None = None
    default_str: str | None = None

    def to_pydantic_field(self) -> tuple[str, tuple[type[str], Any]]:
        field_kwargs: dict[str, Any] = {}
        if self.min_length is not None:
            field_kwargs["min_length"] = self.min_length
        if self.max_length is not None:
            field_kwargs["max_length"] = self.max_length
        if self.description:
            field_kwargs["description"] = self.description

        return self.label.lower(), (str, Field(default=self.default_str, **field_kwargs))

    def to_gql_type(self, extra: dict[str, Any] | None = None) -> "StrFieldGql":
        obj = StrFieldGql.from_pydantic(self, extra=extra)
        return obj


@pyd_type(model=StrField)
class StrFieldGql:
    id: strawberry.auto
    label: strawberry.auto
    description: strawberry.auto
    default_str: strawberry.auto
    min_length: strawberry.auto
    max_length: strawberry.auto
