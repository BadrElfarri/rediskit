from typing import Any, Literal

import strawberry
from pydantic import Field
from strawberry.experimental.pydantic import type as pyd_type
from strawberry.scalars import JSON

from rediskit.typed_fields.base_field import DataTypeFieldBase, FieldTypeEnum


class ListField(DataTypeFieldBase):
    typename__: Literal[FieldTypeEnum.ListField] = Field(default=FieldTypeEnum.ListField, alias="__typename")
    default_list: list[Any] | None = Field(default=None, alias="defaultList")

    def to_pydantic_field(self) -> tuple[str, tuple[type[list[Any]], Any]]:
        field_kwargs: dict[str, Any] = {}

        if self.description:
            field_kwargs["description"] = self.description

        return self.label.lower(), (list[Any], Field(default=self.default_list, **field_kwargs))

    def to_gql_type(self, extra: dict[str, Any] | None = None) -> "ListFieldGql":
        obj = ListFieldGql.from_pydantic(self, extra=extra)
        return obj


@pyd_type(model=ListField)
class ListFieldGql:
    id: strawberry.auto
    label: strawberry.auto
    description: strawberry.auto
    default_list: JSON
