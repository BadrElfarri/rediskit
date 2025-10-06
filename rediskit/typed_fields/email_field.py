from typing import Any, Literal

import strawberry
from pydantic import EmailStr, Field
from strawberry.experimental.pydantic import type as pyd_type

from rediskit.typed_fields.base_field import DataTypeFieldBase, FieldTypeEnum


class EmailField(DataTypeFieldBase):
    typename__: Literal[FieldTypeEnum.EmailField] = Field(default=FieldTypeEnum.EmailField, alias="__typename")
    default_email: EmailStr | None = Field(default=None, alias="defaultEmail")

    def to_pydantic_field(self) -> tuple[str, tuple[type[EmailStr], Any]]:
        field_kwargs: dict[str, Any] = {}

        if self.description:
            field_kwargs["description"] = self.description

        return self.label.lower(), (EmailStr, Field(default=self.default_email, **field_kwargs))

    def to_gql_type(self, extra: dict[str, Any] | None = None) -> "EmailFieldGql":
        obj = EmailFieldGql.from_pydantic(self, extra=extra)
        return obj


@pyd_type(model=EmailField)
class EmailFieldGql:
    id: strawberry.auto
    label: strawberry.auto
    description: strawberry.auto
    default_email: strawberry.auto
