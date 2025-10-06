from typing import Any, Literal

from pydantic import Field

from rediskit import BaseModel
from rediskit.typed_fields.base_field import DataTypeFieldBase, build_dynamic_model


class ObjectField(DataTypeFieldBase):
    typename__: Literal["ObjectField"] = Field(default="ObjectField", alias="__typename")
    fields: list[DataTypeFieldBase]
    value: dict[str, Any] | None = None

    def to_pydantic_field(self) -> tuple[str, tuple[type[BaseModel], Any]]:
        nested_model = build_dynamic_model(f"{self.label.title()}SubModel", self.fields)
        field_kwargs: dict[str, Any] = {}
        if self.description:
            field_kwargs["description"] = self.description
        return self.label.lower(), (nested_model, Field(default=self.value, **field_kwargs))
