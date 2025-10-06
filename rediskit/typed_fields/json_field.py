from typing import Any, Literal

import strawberry
from pydantic import Field
from strawberry.experimental.pydantic import type as pyd_type
from strawberry.scalars import JSON

from rediskit.typed_fields.base_field import DataTypeFieldBase, FieldTypeEnum


class JsonField(DataTypeFieldBase):
    typename__: Literal[FieldTypeEnum.JsonField] = Field(default=FieldTypeEnum.JsonField, alias="__typename")
    default_dict: dict[str, Any] | None = Field(default=None, alias="defaultDict")

    def to_pydantic_field(self) -> tuple[str, tuple[type[dict[str, Any]], Any]]:
        field_kwargs: dict[str, Any] = {}

        if self.description:
            field_kwargs["description"] = self.description

        return self.label.lower(), (dict[str, Any], Field(default=self.default_dict, **field_kwargs))

    def to_gql_type(self, extra: dict[str, Any] | None = None) -> "JsonFieldGql":
        obj = JsonFieldGql.from_pydantic(self, extra=extra)
        return obj


@pyd_type(model=JsonField)
class JsonFieldGql:
    id: strawberry.auto
    label: strawberry.auto
    description: strawberry.auto
    default_dict: JSON
