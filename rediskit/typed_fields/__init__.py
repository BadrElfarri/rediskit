from typing import Annotated, Union

from pydantic import Field

from rediskit.typed_fields.base_field import DataTypeFieldBase, FieldTypeEnum
from rediskit.typed_fields.bool_field import BoolField, BoolFieldGql
from rediskit.typed_fields.date_field import DateField, DateFieldGql, DateTimeField, DateTimeFieldGql
from rediskit.typed_fields.email_field import EmailField, EmailFieldGql
from rediskit.typed_fields.enum_field import EnumField, EnumFieldGql
from rediskit.typed_fields.float_field import FloatField, FloatFieldGql
from rediskit.typed_fields.from_func import build_dynamic_model, build_model_from_function, fields_from_function
from rediskit.typed_fields.int_field import IntField, IntFieldGql
from rediskit.typed_fields.json_field import JsonField, JsonFieldGql
from rediskit.typed_fields.list_field import ListField, ListFieldGql
from rediskit.typed_fields.object_field import ObjectField, ObjectFieldGql
from rediskit.typed_fields.str_field import StrField, StrFieldGql
from rediskit.typed_fields.uuid_field import UuidField, UuidFieldGql

TypeFieldsUnionGql = (
    BoolFieldGql
    | DateFieldGql
    | DateTimeFieldGql
    | EmailFieldGql
    | EnumFieldGql
    | FloatFieldGql
    | IntFieldGql
    | JsonFieldGql
    | ListFieldGql
    | StrFieldGql
    | UuidFieldGql
    | ObjectFieldGql
)
TypeFieldsUnion = Annotated[
    Union[
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
    "build_model_from_function",
    "fields_from_function",
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
