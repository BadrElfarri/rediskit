import uuid
from datetime import date, datetime
from enum import Enum
from typing import Any, Sequence

from pydantic import EmailStr, Field, create_model

from rediskit import BaseModel
from rediskit.utils import uuid_7


class FieldTypeEnum(str, Enum):
    UuidField = "UuidField"
    StrField = "StringField"
    EmailField = "EmailField"
    IntField = "IntField"
    FloatField = "FloatField"
    BoolField = "BooleanField"
    DateField = "DateField"
    DateTimeField = "DateTimeField"
    JsonField = "JsonField"
    ListField = "ListField"
    EnumField = "EnumField"

    def to_py_type(self) -> Any:
        type_mapping = {
            FieldTypeEnum.UuidField: uuid.UUID,
            FieldTypeEnum.StrField: str,
            FieldTypeEnum.EmailField: EmailStr,
            FieldTypeEnum.IntField: int,
            FieldTypeEnum.FloatField: float,
            FieldTypeEnum.BoolField: bool,
            FieldTypeEnum.DateField: date,
            FieldTypeEnum.DateTimeField: datetime,
            FieldTypeEnum.JsonField: dict[str, Any],
            FieldTypeEnum.ListField: list[Any],
        }
        return type_mapping[self]


class DataTypeFieldBase(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid_7()))
    label: str
    description: str | None = None


def build_dynamic_model(name: str, fields: Sequence[DataTypeFieldBase]) -> type[BaseModel]:
    field_defs: dict[str, Any] = {}

    for field in fields:
        if hasattr(field, "to_pydantic_field"):
            key, (typ, field_def) = field.to_pydantic_field()
            field_defs[key] = (typ, field_def)

    return create_model(name, **field_defs)
