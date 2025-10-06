import inspect
import types
import typing as t
import uuid
from datetime import date, datetime
from typing import Literal, Union, get_args, get_origin

from rediskit.typed_fields.base_field import build_dynamic_model
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

NoneType = type(None)


def _unwrap_optional(tp):
    """Return (inner_type, is_optional)."""
    origin = get_origin(tp)
    if origin in (Union, types.UnionType):  # typing.Union and X | Y
        args = tuple(get_args(tp))
        if NoneType in args and len(args) == 2:
            inner = next(a for a in args if a is not NoneType)
            return inner, True
    return tp, False


def _default_kw_for(field_cls):
    """Map field class → default_* kwarg name."""
    match field_cls:
        case StrField():
            return "default_str"
        case IntField():
            return "default_int"
        case FloatField():
            return "default_float"
        case BoolField():
            return "default_bool"
        case DateField():
            return "default_date"
        case DateTimeField():
            return "default_datetime"
        case EmailField():
            return "default_email"
        case UuidField():
            return "default_uuid"
        case JsonField():
            return "default_dict"
        case ListField():
            return "default_list"
        case EnumField():
            return "default_str"
        case _:
            return None  # ObjectField or unknown field type


def _is_typed_mapping(tp) -> bool:
    return get_origin(tp) in (dict, t.Dict, dict)


def _is_typed_list(tp) -> bool:
    return get_origin(tp) in (list, t.List, list)


def _is_literal(tp) -> bool:
    return get_origin(tp) is Literal


def _is_object_like(tp) -> bool:
    # Simple heuristic: dataclass-style or TypedDict-ish classes with __annotations__
    return hasattr(tp, "__annotations__") and isinstance(getattr(tp, "__annotations__"), dict)


def _choose_field_for(name: str, ann: t.Any):
    """
    Return (field_cls, field_kwargs_factory) where field_kwargs_factory(default_value) -> kwargs.
    """
    ann, _is_opt = _unwrap_optional(ann)

    # Literal[...] → EnumField
    if _is_literal(ann):
        allowed_values = list(get_args(ann))

        def f(default):
            kw = {"allowed_values": allowed_values}
            if default is not inspect._empty:
                kw["default_str"] = default
            return EnumField, kw

        return f  # <- not f(None)

    # list[T] → ListField
    if _is_typed_list(ann):

        def f(default):
            kw = {}
            if default is not inspect._empty:
                kw["default_list"] = default
            return ListField, kw

        return f

    # dict[...] → JsonField
    if _is_typed_mapping(ann):

        def f(default):
            kw = {}
            if default is not inspect._empty:
                kw["default_dict"] = default
            return JsonField, kw

        return f

    # UUID
    if ann in (uuid.UUID,):

        def f(default):
            kw = {}
            if default is not inspect._empty:
                kw["default_uuid"] = default
            return UuidField, kw

        return f

    # Date / DateTime
    if ann in (date,):

        def f(default):
            kw = {}
            if default is not inspect._empty:
                kw["default_date"] = default
            return DateField, kw

        return f
    if ann in (datetime,):

        def f(default):
            kw = {}
            if default is not inspect._empty:
                kw["default_datetime"] = default
            return DateTimeField, kw

        return f

    # bool / int / float
    if ann in (bool,):

        def f(default):
            kw = {}
            if default is not inspect._empty:
                kw["default_bool"] = default
            return BoolField, kw

        return f
    if ann in (int,):

        def f(default):
            kw = {}
            if default is not inspect._empty:
                kw["default_int"] = default
            return IntField, kw

        return f
    if ann in (float,):

        def f(default):
            kw = {}
            if default is not inspect._empty:
                kw["default_float"] = default
            return FloatField, kw

        return f

    # object-like → ObjectField (recurse)
    if _is_object_like(ann):

        def f(default):
            inner_fields = _fields_from_annotations(ann.__annotations__)
            return ObjectField, {"fields": inner_fields}

        return f

    # Heuristic: str named like email → EmailField; else StrField
    if ann in (str,):

        def f(default):
            kw = {}
            if "email" in name.lower():
                if default is not inspect._empty:
                    kw["default_email"] = default
                return EmailField, kw
            else:
                if default is not inspect._empty:
                    kw["default_str"] = default
                return StrField, kw

        return f

    # Fallback to JsonField
    def f(default):
        kw = {}
        if default is not inspect._empty:
            kw["default_dict"] = default if isinstance(default, dict) else {"value": default}
        return JsonField, kw

    return f


def _fields_from_annotations(annotations: dict[str, t.Any], defaults: dict[str, t.Any] | None = None, overrides: dict[str, dict] | None = None):
    defaults = defaults or {}
    overrides = overrides or {}
    fields = []
    for name, ann in annotations.items():
        ov = overrides.get(name, {})
        # allow passing a fully constructed Field via overrides["name"]["field"]
        field_instance = ov.get("field")
        if field_instance is not None:
            fields.append(field_instance)
            continue

        default = defaults.get(name, inspect._empty)
        chooser = _choose_field_for(name, ann)
        if callable(chooser):
            field_cls, base_kwargs = chooser(default)  # function form
        else:
            field_cls, base_kwargs = chooser
            # Merge manual overrides
        merged = {**base_kwargs, **{k: v for k, v in ov.items() if k != "field"}}
        fields.append(field_cls(label=name, **merged))
    return fields


def fields_from_function(func: t.Callable, overrides: dict[str, dict] | None = None):
    sig = inspect.signature(func)
    annotations = {k: p.annotation for k, p in sig.parameters.items() if k != "self"}
    defaults = {k: (p.default if p.default is not inspect._empty else inspect._empty) for k, p in sig.parameters.items() if k != "self"}
    return _fields_from_annotations(annotations, defaults=defaults, overrides=overrides)


def build_model_from_function(func: t.Callable, *, name: str | None = None, overrides: dict[str, dict] | None = None):
    model_name = name or f"{func.__name__.capitalize()}Model"
    flds = fields_from_function(func, overrides=overrides)
    return build_dynamic_model(model_name, flds)
