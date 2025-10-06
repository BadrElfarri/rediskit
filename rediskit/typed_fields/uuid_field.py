import uuid
from typing import Any, Literal

from pydantic import Field

from rediskit.typed_fields.base_field import DataTypeFieldBase, FieldTypeEnum


class UuidField(DataTypeFieldBase):
    typename__: Literal[FieldTypeEnum.UuidField] = Field(default=FieldTypeEnum.UuidField, alias="__typename")
    value: uuid.UUID | None = None

    def to_pydantic_field(self) -> tuple[str, tuple[type[uuid.UUID], Any]]:
        field_kwargs: dict[str, Any] = {}

        if self.description:
            field_kwargs["description"] = self.description

        return self.label.lower(), (uuid.UUID, Field(default=self.value, **field_kwargs))
