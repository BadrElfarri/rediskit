import datetime
import enum
import json
import uuid
from typing import Any, Dict

from fastapi.responses import JSONResponse
from pydantic import BaseModel as pyBaseModel
from pydantic import ConfigDict, ValidationError


class CustomJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        serializable_content = serialize_values(content)
        return json.dumps(serializable_content, ensure_ascii=False, allow_nan=False).encode("utf-8")


def custom_json_serializer(obj: object) -> str:
    return json.dumps(serialize_values(obj))


def custom_json_deserializer(s: str) -> Any:
    return json.loads(s)


class BaseModel(pyBaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        validate_assignment=True,
        arbitrary_types_allowed=True,
    )

    def __init__(self, **kwargs: Any):
        """

        @rtype: object
        """
        super().__init__(**kwargs)

    def dump(
        self,
        keep_data_types: bool = True,
        exclude_unset: bool = True,
        exclude_none: bool = True,
        enum_as_name: bool = False,
    ) -> Dict[str, Any]:
        data = self.model_dump(exclude_unset=exclude_unset, exclude_none=exclude_none)
        if not keep_data_types:
            data = serialize_values(data, enum_as_name)
        return data

    def merged_with(
        self,
        other: "BaseModel",
        *,
        exclude_unset: bool = False,  # default True is usually what you want
        exclude_none: bool = True,
    ) -> "BaseModel":
        if type(self) is not type(other):
            raise TypeError(f"Cannot merge {type(other)} into {type(self)}")

        # work on a deep copy so we don't mutate self
        result = self.model_copy(deep=True)

        # only iterate fields we intend to update
        for name in type(self).model_fields:
            if exclude_unset and name not in other.model_fields_set:
                continue

            inc = getattr(other, name, None)
            if exclude_none and inc is None:
                continue

            cur = getattr(result, name, None)
            merged_val = self._merge_value(name, cur, inc)
            setattr(result, name, merged_val)

        return result

    def update_from(
        self,
        other: "BaseModel",
        *,
        exclude_unset: bool = False,
        exclude_none: bool = True,
    ) -> "BaseModel":
        merged = self.merged_with(other, exclude_unset=exclude_unset, exclude_none=exclude_none)
        for name in type(self).model_fields:
            setattr(self, name, getattr(merged, name))
        return self

    def _merge_value(self, field: str, current: Any, incoming: Any) -> Any:
        """
        Default rules:
        - if incoming is None -> keep current
        - if both dict -> shallow union
        - if both BaseModel -> recursive merge
        - else -> replace with incoming
        """
        if incoming is None:
            return current

        # dict union
        if isinstance(current, dict) and isinstance(incoming, dict):
            return {**current, **incoming}

        # nested model: recurse
        if isinstance(current, BaseModel) and isinstance(incoming, BaseModel):
            return current.merged_with(incoming, exclude_unset=True, exclude_none=True)

        if isinstance(current, list) and isinstance(incoming, list):
            to_add = []
            for item in incoming:
                if (item in to_add) or (item in current):
                    continue
                to_add.append(item)
            return current + to_add
        # default: replace
        return incoming

    @classmethod
    def safe_validate(cls, data: dict):
        try:
            return cls.model_validate(data)
        except ValidationError as e:
            values: dict[str | int, Any] = {}
            # for each error, just set field to None
            for err in e.errors():
                field = err["loc"][0]
                values[field] = None
            m = cls(**(data | values))
            return m


# TODO: this is "double" defined else where fix it
def json_encoder(value: Any, raiseIfNoMatch: bool = False, enum_as_name: bool = False) -> Any:
    if isinstance(value, enum.Enum):
        if enum_as_name:
            return value.name
        else:
            return value.value
    elif isinstance(value, uuid.UUID):
        return str(value)
    elif isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    elif isinstance(value, BaseModel):
        return value.dump(keep_data_types=False, exclude_none=True, exclude_unset=False)
    elif isinstance(value, pyBaseModel):
        return value.model_dump(exclude_none=True, exclude_unset=False)
    elif raiseIfNoMatch:
        raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")
    return value


# TODO: this is "double" defined else where fix it
def serialize_values(value: Any, enum_as_name: bool = False) -> Any:
    if isinstance(value, dict):
        serializedDict = {}
        for dictKey, dictValue in value.items():
            serializedKey = json_encoder(dictKey, enum_as_name=enum_as_name)
            serializedDict[serializedKey] = serialize_values(dictValue, enum_as_name=enum_as_name)
        return serializedDict
    elif isinstance(value, list):
        serializedList = list(map(lambda listValue: serialize_values(listValue, enum_as_name=enum_as_name), value))
        return serializedList
    return json_encoder(value, enum_as_name=enum_as_name)
