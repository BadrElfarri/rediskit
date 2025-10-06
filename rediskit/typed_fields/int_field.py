from typing import Any, Literal

from pydantic import Field

from rediskit.typed_fields.base_field import DataTypeFieldBase, FieldTypeEnum


class IntField(DataTypeFieldBase):
    typename__: Literal[FieldTypeEnum.IntField] = Field(default=FieldTypeEnum.IntField, alias="__typename")
    ge: int | None = None
    le: int | None = None
    value: int | None = None

    def to_pydantic_field(self) -> tuple[str, tuple[type[int], Any]]:
        field_kwargs: dict[str, Any] = {}
        if self.ge is not None:
            field_kwargs["ge"] = self.ge
        if self.le is not None:
            field_kwargs["le"] = self.le
        if self.description:
            field_kwargs["description"] = self.description

        return self.label.lower(), (int, Field(default=self.value, **field_kwargs))
