import base64
import datetime
import enum
import json
import uuid
from typing import Any

from uuid_utils import uuid7


def base64_json_to_dict(keys_base64: str | None) -> dict[str, str]:
    if not keys_base64:
        raise ValueError("Key is None.")
    try:
        # Decode from base64 to a JSON string, then load it into a dict
        decodedJson = base64.b64decode(keys_base64).decode("utf-8")
        decoded = json.loads(decodedJson)
        return decoded
    except Exception as e:
        raise ValueError("Invalid key format") from e


def json_encoder(value: Any, raise_if_no_match: bool = False):
    if isinstance(value, enum.Enum):
        return value.value
    elif isinstance(value, uuid.UUID):
        return str(value)
    elif isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    elif raise_if_no_match:
        raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")
    return value


def serialize_values(value: Any) -> Any:
    if isinstance(value, dict):
        serialized_dict = {}
        for dictKey, dictValue in value.items():
            serialized_key = json_encoder(dictKey)
            serialized_dict[serialized_key] = serialize_values(dictValue)
        return serialized_dict
    elif isinstance(value, list):
        serialized_list = [serialize_values(v) for v in value]
        return serialized_list
    return json_encoder(value)


def dict_to_list(items: dict | None) -> list[Any]:
    list_items: list[Any] = []
    if items is None:
        return list_items
    for key in items:
        list_items.append(items[key])
    return list_items


def deserialize_dict_model_property(items: dict | None, model_type: Any) -> None:
    if isinstance(items, dict):
        for key in items:
            value = items[key]
            if isinstance(value, dict):
                items[key] = model_type(**value)


def merge_dict_data(old_dict: dict, new_dict: dict) -> dict:
    merged_dict = dict(old_dict)
    for key in new_dict:
        if key in merged_dict:
            if isinstance(merged_dict[key], dict) and isinstance(new_dict[key], dict):
                merged_dict[key] = merge_dict_data(merged_dict[key], new_dict[key])
            else:
                merged_dict[key] = new_dict[key]
        else:
            merged_dict[key] = new_dict[key]
    return merged_dict


def remove_nones_from_dict_data(original_dict: dict) -> dict:
    clean_dict = dict(original_dict)
    keys_to_pop = []
    for key in clean_dict:
        if isinstance(clean_dict[key], dict):
            clean_dict[key] = remove_nones_from_dict_data(clean_dict[key])
        else:
            if clean_dict[key] is None:
                keys_to_pop.append(key)
    for key_to_pop in keys_to_pop:
        clean_dict.pop(key_to_pop)
    return clean_dict


def remove_matching_dict_data(original_dict: dict, matching_dict: dict) -> tuple[dict, dict]:
    changed_data = {}
    clean_dict = dict(matching_dict)
    keys_to_pop = []
    for key in clean_dict:
        if key in original_dict:
            if isinstance(original_dict[key], dict) and isinstance(clean_dict[key], dict):
                data = remove_matching_dict_data(original_dict[key], clean_dict[key])
                clean_dict[key] = data[0]
                changed_data[key] = data[1]
            elif isinstance(original_dict[key], list) and isinstance(matching_dict[key], list):
                if check_matching_list_data(original_dict[key], matching_dict[key]):
                    keys_to_pop.append(key)
                else:
                    changed_data[key] = original_dict[key]
            else:
                if original_dict[key] == clean_dict[key]:
                    keys_to_pop.append(key)
                else:
                    changed_data[key] = original_dict[key]
    for key_to_pop in keys_to_pop:
        clean_dict.pop(key_to_pop)
    return clean_dict, changed_data


def check_matching_dict_data(original_dict: dict, matching_dict: dict) -> bool:
    matching = True
    for key in matching_dict:
        if key in original_dict:
            if isinstance(original_dict[key], dict) and isinstance(matching_dict[key], dict):
                matching = check_matching_dict_data(original_dict[key], matching_dict[key])
            elif isinstance(original_dict[key], list) and isinstance(matching_dict[key], list):
                matching = check_matching_list_data(original_dict[key], matching_dict[key])
            else:
                matching = original_dict[key] == matching_dict[key]
        else:
            matching = False
        if not matching:
            break
    return matching


def check_empty_dict_data(data: dict) -> bool:
    empty = True
    for key in data:
        if isinstance(data[key], dict):
            empty = check_empty_dict_data(data[key])
        else:
            empty = False
        if not empty:
            break
    return empty


def check_matching_list_data(original_list: list, matching_list: list) -> bool:
    matching = len(original_list) == len(matching_list)
    for i in range(len(original_list)):
        if not matching:
            break
        if isinstance(original_list[i], dict) and isinstance(matching_list[i], dict):
            matching = check_matching_dict_data(original_list[i], matching_list[i])
        elif isinstance(original_list[i], list) and isinstance(matching_list[i], list):
            matching = check_matching_list_data(original_list[i], matching_list[i])
        else:
            matching = original_list[i] == matching_list[i]
    return matching


def remove_keys(data: dict, key_map: dict, ignore_keys: list[str] | None = None) -> None:
    if ignore_keys is None:
        ignore_keys = ["id"]
    for key in key_map:
        if key in data and key not in ignore_keys:
            if isinstance(key_map[key], dict):
                if isinstance(data[key], dict):
                    remove_keys(data[key], key_map[key])
            else:
                data.pop(key)


def uuid_7() -> uuid.UUID:
    # Note: Not part of standard python will be added at python 3.14. Using this for now as primary key
    return uuid.UUID(str(uuid7()))
