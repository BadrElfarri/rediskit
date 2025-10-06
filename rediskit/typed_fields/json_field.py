from typing import Any, Literal

from pydantic import Field

from rediskit.typed_fields.base_field import DataTypeFieldBase, FieldTypeEnum


class JsonField(DataTypeFieldBase):
    typename__: Literal[FieldTypeEnum.JsonField] = Field(default=FieldTypeEnum.JsonField, alias="__typename")
    value: dict[str, Any] | None = None

    def to_pydantic_field(self) -> tuple[str, tuple[type[dict[str, Any]], Any]]:
        field_kwargs: dict[str, Any] = {}

        if self.description:
            field_kwargs["description"] = self.description

        return self.label.lower(), (dict[str, Any], Field(default=self.value, **field_kwargs))
