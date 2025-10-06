from typing import Any, Literal

import strawberry
from pydantic import Field
from strawberry.experimental.pydantic import type as pyd_type

from rediskit.typed_fields.base_field import DataTypeFieldBase, FieldTypeEnum


class IntField(DataTypeFieldBase):
    typename__: Literal[FieldTypeEnum.IntField] = Field(default=FieldTypeEnum.IntField, alias="__typename")
    ge_int: int | None = Field(default=None, alias="geInt")
    le_int: int | None = Field(default=None, alias="leInt")
    default_int: int | None = Field(default=None, alias="defaultInt")

    def to_pydantic_field(self) -> tuple[str, tuple[type[int], Any]]:
        field_kwargs: dict[str, Any] = {}
        if self.ge_int is not None:
            field_kwargs["ge"] = self.ge_int
        if self.le_int is not None:
            field_kwargs["le"] = self.le_int
        if self.description:
            field_kwargs["description"] = self.description

        return self.label.lower(), (int, Field(default=self.default_int, **field_kwargs))

    def to_gql_type(self, extra: dict[str, Any] | None = None) -> "IntFieldGql":
        obj = IntFieldGql.from_pydantic(self, extra=extra)
        return obj


@pyd_type(model=IntField)
class IntFieldGql:
    id: strawberry.auto
    label: strawberry.auto
    description: strawberry.auto
    default_int: strawberry.auto
    ge_int: strawberry.auto
    le_int: strawberry.auto
