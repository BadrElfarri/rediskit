from typing import Any, Literal

import strawberry
from pydantic import Field
from strawberry.experimental.pydantic import type as pyd_type
from strawberry.scalars import JSON

from rediskit import BaseModel
from rediskit.typed_fields.base_field import DataTypeFieldBase, build_dynamic_model


class ObjectField(DataTypeFieldBase):
    typename__: Literal["ObjectField"] = Field(default="ObjectField", alias="__typename")
    fields: list[DataTypeFieldBase]

    def to_pydantic_field(self) -> tuple[str, tuple[type[BaseModel], Any]]:
        nested_model = build_dynamic_model(f"{self.label.title()}SubModel", self.fields)
        field_kwargs: dict[str, Any] = {}
        if self.description:
            field_kwargs["description"] = self.description
        return self.label.lower(), (nested_model, Field(**field_kwargs))

    def to_gql_type(self, extra: dict[str, Any] | None = None) -> "ObjectFieldGql":
        obj = ObjectFieldGql.from_pydantic(self, extra=extra)
        return obj


@pyd_type(model=ObjectField)
class ObjectFieldGql:
    id: strawberry.auto
    label: strawberry.auto
    description: strawberry.auto
    fields: JSON
