from typing import Any, Literal

import strawberry
from pydantic import Field
from strawberry.experimental.pydantic import type as pyd_type

from rediskit.typed_fields.base_field import DataTypeFieldBase, FieldTypeEnum


class FloatField(DataTypeFieldBase):
    typename__: Literal[FieldTypeEnum.FloatField] = Field(default=FieldTypeEnum.FloatField, alias="__typename")
    ge_float: float | None = Field(default=None, alias="geFloat")
    le_float: float | None = Field(default=None, alias="leFloat")
    default_float: float | None = Field(default=None, alias="defaultFloat")

    def to_pydantic_field(self) -> tuple[str, tuple[type[float], Any]]:
        field_kwargs: dict[str, Any] = {}
        if self.ge_float is not None:
            field_kwargs["ge"] = self.ge_float
        if self.le_float is not None:
            field_kwargs["le"] = self.le_float
        if self.description:
            field_kwargs["description"] = self.description

        return self.label.lower(), (float, Field(default=self.default_float, **field_kwargs))

    def to_gql_type(self, extra: dict[str, Any] | None = None) -> "FloatFieldGql":
        obj = FloatFieldGql.from_pydantic(self, extra=extra)
        return obj


@pyd_type(model=FloatField)
class FloatFieldGql:
    id: strawberry.auto
    label: strawberry.auto
    description: strawberry.auto
    default_float: strawberry.auto
    ge_float: strawberry.auto
    le_float: strawberry.auto
