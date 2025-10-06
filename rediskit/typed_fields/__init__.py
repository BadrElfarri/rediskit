from typing import Annotated, Union

from pydantic import Field

from rediskit.typed_fields.base_field import DataTypeFieldBase, FieldTypeEnum, build_dynamic_model
from rediskit.typed_fields.bool_field import BoolField
from rediskit.typed_fields.date_field import DateField, DateTimeField
from rediskit.typed_fields.email_field import EmailField
from rediskit.typed_fields.enum_field import EnumField
from rediskit.typed_fields.float_field import FloatField
from rediskit.typed_fields.int_field import IntField
from rediskit.typed_fields.json_field import JsonField
from rediskit.typed_fields.list_field import ListField
from rediskit.typed_fields.object_field import ObjectField
from rediskit.typed_fields.str_field import StrField
from rediskit.typed_fields.uuid_field import UuidField

TypeFieldsUnion = Annotated[
    Union[
        "FieldTypeEnum",
        "BoolField",
        "DateField",
        "DateTimeField",
        "EmailField",
        "EnumField",
        "FloatField",
        "IntField",
        "JsonField",
        "ListField",
        "StrField",
        "UuidField",
        "ObjectField",
    ],
    Field(discriminator="typename__"),
]

__all__ = [
    "build_dynamic_model",
    "FieldTypeEnum",
    "DataTypeFieldBase",
    "BoolField",
    "DateField",
    "DateTimeField",
    "EmailField",
    "EnumField",
    "FloatField",
    "IntField",
    "JsonField",
    "ListField",
    "StrField",
    "UuidField",
    "ObjectField",
]
