from datetime import date, datetime
from typing import Any, Literal

import strawberry
from pydantic import Field
from strawberry.experimental.pydantic import type as pyd_type

from rediskit.typed_fields.base_field import DataTypeFieldBase, FieldTypeEnum


class DateField(DataTypeFieldBase):
    typename__: Literal[FieldTypeEnum.DateField] = Field(default=FieldTypeEnum.DateField, alias="__typename")
    default_date: date | None = Field(default=None, alias="efaultDate")

    def to_pydantic_field(self) -> tuple[str, tuple[type[date], Any]]:
        return self.label.lower(), (date, Field(default=self.default_date))

    def to_gql_type(self, extra: dict[str, Any] | None = None) -> "DateFieldGql":
        obj = DateFieldGql.from_pydantic(self, extra=extra)
        return obj


@pyd_type(model=DateField)
class DateFieldGql:
    id: strawberry.auto
    label: strawberry.auto
    description: strawberry.auto
    default_date: strawberry.auto


class DateTimeField(DataTypeFieldBase):
    typename__: Literal[FieldTypeEnum.DateTimeField] = Field(default=FieldTypeEnum.DateTimeField, alias="__typename")
    default_datetime: datetime | None = Field(default=None, alias="defaultDatetime")

    def to_pydantic_field(self) -> tuple[str, tuple[type[datetime], Any]]:
        field_kwargs: dict[str, Any] = {}

        if self.description:
            field_kwargs["description"] = self.description

        return self.label.lower(), (datetime, Field(default=self.default_datetime, **field_kwargs))

    def to_gql_type(self, extra: dict[str, Any] | None = None) -> "DateTimeFieldGql":
        obj = DateTimeFieldGql.from_pydantic(self, extra=extra)
        return obj


@pyd_type(model=DateTimeField)
class DateTimeFieldGql:
    id: strawberry.auto
    label: strawberry.auto
    description: strawberry.auto
    default_datetime: strawberry.auto
