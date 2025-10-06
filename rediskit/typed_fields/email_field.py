from typing import Any, Literal

from pydantic import EmailStr, Field

from rediskit.typed_fields.base_field import DataTypeFieldBase, FieldTypeEnum


class EmailField(DataTypeFieldBase):
    typename__: Literal[FieldTypeEnum.EmailField] = Field(default=FieldTypeEnum.EmailField, alias="__typename")
    value: EmailStr | None = None

    def to_pydantic_field(self) -> tuple[str, tuple[type[EmailStr], Any]]:
        field_kwargs: dict[str, Any] = {}

        if self.description:
            field_kwargs["description"] = self.description

        return self.label.lower(), (EmailStr, Field(default=self.value, **field_kwargs))
