from typing import Any, Literal

from pydantic import Field

from rediskit.typed_fields.base_field import DataTypeFieldBase, FieldTypeEnum


class ListField(DataTypeFieldBase):
    typename__: Literal[FieldTypeEnum.ListField] = Field(default=FieldTypeEnum.ListField, alias="__typename")
    value: list[Any] | None = None

    def to_pydantic_field(self) -> tuple[str, tuple[type[list[Any]], Any]]:
        field_kwargs: dict[str, Any] = {}

        if self.description:
            field_kwargs["description"] = self.description

        return self.label.lower(), (list[Any], Field(default=self.value, **field_kwargs))
