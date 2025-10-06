from typing import Any, Literal

from pydantic import Field

from rediskit.typed_fields.base_field import DataTypeFieldBase, FieldTypeEnum


class FloatField(DataTypeFieldBase):
    typename__: Literal[FieldTypeEnum.FloatField] = Field(default=FieldTypeEnum.FloatField, alias="__typename")
    ge: float | None = None
    le: float | None = None
    value: float | None = None

    def to_pydantic_field(self) -> tuple[str, tuple[type[float], Any]]:
        field_kwargs: dict[str, Any] = {}
        if self.ge is not None:
            field_kwargs["ge"] = self.ge
        if self.le is not None:
            field_kwargs["le"] = self.le
        if self.description:
            field_kwargs["description"] = self.description

        return self.label.lower(), (float, Field(default=self.value, **field_kwargs))
