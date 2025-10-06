from typing import Any, Literal

from pydantic import Field

from rediskit.typed_fields.base_field import DataTypeFieldBase, FieldTypeEnum


class BoolField(DataTypeFieldBase):
    typename__: Literal[FieldTypeEnum.BoolField] = Field(default=FieldTypeEnum.BoolField, alias="__typename")
    value: bool | None = None

    def to_pydantic_field(self) -> tuple[str, tuple[type[bool], Any]]:
        field_kwargs: dict[str, Any] = {}

        if self.description:
            field_kwargs["description"] = self.description

        return self.label.lower(), (bool, Field(default=self.value, **field_kwargs))
