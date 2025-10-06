from typing import Any, Literal

from pydantic import Field

from rediskit.typed_fields.base_field import DataTypeFieldBase, FieldTypeEnum


class StrField(DataTypeFieldBase):
    typename__: Literal[FieldTypeEnum.StrField] = Field(default=FieldTypeEnum.StrField, alias="__typename")
    min_length: int | None = None
    max_length: int | None = None
    value: str | None = None

    def to_pydantic_field(self) -> tuple[str, tuple[type[str], Any]]:
        field_kwargs: dict[str, Any] = {}
        if self.min_length is not None:
            field_kwargs["min_length"] = self.min_length
        if self.max_length is not None:
            field_kwargs["max_length"] = self.max_length
        if self.description:
            field_kwargs["description"] = self.description

        return self.label.lower(), (str, Field(default=self.value, **field_kwargs))
