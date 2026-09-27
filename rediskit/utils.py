import base64
import datetime
import enum
import json
import logging
import uuid
from typing import Any

log = logging.getLogger(__name__)

_REDIS_GLOB_CHARS = frozenset("*?[]\\")


def has_glob_pattern(value: str) -> bool:
    return any(ch in _REDIS_GLOB_CHARS for ch in value)


def base64_json_to_dict(keys_base64: str | None) -> dict[str, str]:
    if not keys_base64:
        # Not configured — fine for users who don't need encryption.
        log.debug("No keys_base64 provided")
        return {}
    try:
        # Decode from base64 to a JSON string, then load it into a dict
        decodedJson = base64.b64decode(keys_base64).decode("utf-8")
        decoded = json.loads(decodedJson)
        return decoded
    except Exception:
        log.exception("Failed to decode keys_base64 as base64-encoded JSON")

    return {}


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
    for key, new_value in new_dict.items():
        if key in merged_dict:
            if isinstance(merged_dict[key], dict) and isinstance(new_value, dict):
                merged_dict[key] = merge_dict_data(merged_dict[key], new_value)
            else:
                merged_dict[key] = new_value
        else:
            merged_dict[key] = new_value
    return merged_dict


def remove_nones_from_dict_data(original_dict: dict) -> dict:
    clean_dict = dict(original_dict)
    keys_to_pop = []
    for key, value in clean_dict.items():
        if isinstance(value, dict):
            clean_dict[key] = remove_nones_from_dict_data(value)
        elif value is None:
            keys_to_pop.append(key)
    for key_to_pop in keys_to_pop:
        clean_dict.pop(key_to_pop)
    return clean_dict


def remove_matching_dict_data(original_dict: dict, matching_dict: dict) -> tuple[dict, dict]:
    changed_data = {}
    clean_dict = dict(matching_dict)
    keys_to_pop = []
    for key, value in clean_dict.items():
        if key in original_dict:
            original_value = original_dict[key]
            if isinstance(original_value, dict) and isinstance(value, dict):
                data = remove_matching_dict_data(original_value, value)
                clean_dict[key] = data[0]
                changed_data[key] = data[1]
            elif isinstance(original_value, list) and isinstance(value, list):
                if check_matching_list_data(original_value, value):
                    keys_to_pop.append(key)
                else:
                    changed_data[key] = original_value
            else:
                if original_value == value:
                    keys_to_pop.append(key)
                else:
                    changed_data[key] = original_value
    for key_to_pop in keys_to_pop:
        clean_dict.pop(key_to_pop)
    return clean_dict, changed_data


def check_matching_dict_data(original_dict: dict, matching_dict: dict) -> bool:
    matching = True
    for key, matching_value in matching_dict.items():
        if key in original_dict:
            original_value = original_dict[key]
            if isinstance(original_value, dict) and isinstance(matching_value, dict):
                matching = check_matching_dict_data(original_value, matching_value)
            elif isinstance(original_value, list) and isinstance(matching_value, list):
                matching = check_matching_list_data(original_value, matching_value)
            else:
                matching = original_value == matching_value
        else:
            matching = False
        if not matching:
            break
    return matching


def check_empty_dict_data(data: dict) -> bool:
    empty = True
    for value in data.values():
        if isinstance(value, dict):
            empty = check_empty_dict_data(value)
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
    for key, sub_map in key_map.items():
        if key in data and key not in ignore_keys:
            if isinstance(sub_map, dict):
                if isinstance(data[key], dict):
                    remove_keys(data[key], sub_map)
            else:
                data.pop(key)
