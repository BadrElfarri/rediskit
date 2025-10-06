from datetime import date, datetime
from typing import Any, Literal

from pydantic import Field

from rediskit.typed_fields.base_field import DataTypeFieldBase, FieldTypeEnum


class DateField(DataTypeFieldBase):
    typename__: Literal[FieldTypeEnum.DateField] = Field(default=FieldTypeEnum.DateField, alias="__typename")
    value: date | None = None

    def to_pydantic_field(self) -> tuple[str, tuple[type[date], Any]]:
        return self.label.lower(), (date, Field(default=self.value))


class DateTimeField(DataTypeFieldBase):
    typename__: Literal[FieldTypeEnum.DateTimeField] = Field(default=FieldTypeEnum.DateTimeField, alias="__typename")
    value: datetime | None = None

    def to_pydantic_field(self) -> tuple[str, tuple[type[datetime], Any]]:
        field_kwargs: dict[str, Any] = {}

        if self.description:
            field_kwargs["description"] = self.description

        return self.label.lower(), (datetime, Field(default=self.value, **field_kwargs))
