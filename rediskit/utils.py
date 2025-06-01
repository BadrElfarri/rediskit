import base64
import datetime
import enum
import json
import uuid
from typing import Any


def base64JsonToDict(keysBase64: str | None) -> dict[str, str]:
    if not keysBase64:
        raise ValueError("Key is None.")
    try:
        # Decode from base64 to a JSON string, then load it into a dict
        decodedJson = base64.b64decode(keysBase64).decode("utf-8")
        decoded = json.loads(decodedJson)
        return decoded
    except Exception as e:
        raise ValueError("Invalid key format") from e


def JsonEncoder(value: Any, raiseIfNoMatch: bool = False):
    if isinstance(value, enum.Enum):
        return value.value
    elif isinstance(value, uuid.UUID):
        return str(value)
    elif isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    elif raiseIfNoMatch:
        raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")
    return value


def SerializeValues(value: Any) -> Any:
    if isinstance(value, dict):
        serializedDict = {}
        for dictKey, dictValue in value.items():
            serializedKey = JsonEncoder(dictKey)
            serializedDict[serializedKey] = SerializeValues(dictValue)
        return serializedDict
    elif isinstance(value, list):
        serializedList = [SerializeValues(v) for v in value]
        return serializedList
    return JsonEncoder(value)


def DictToList(items: dict | None) -> list[Any]:
    listItems: list[Any] = []
    if items is None:
        return listItems
    for key in items:
        listItems.append(items[key])
    return listItems


def DeserializeDictModelProperty(items: dict | None, modelType: Any) -> None:
    if isinstance(items, dict):
        for key in items:
            value = items[key]
            if isinstance(value, dict):
                items[key] = modelType(**value)


def MergeDictData(oldDict: dict, newDict: dict) -> dict:
    mergedDict = dict(oldDict)
    for key in newDict:
        if key in mergedDict:
            if isinstance(mergedDict[key], dict) and isinstance(newDict[key], dict):
                mergedDict[key] = MergeDictData(mergedDict[key], newDict[key])
            else:
                mergedDict[key] = newDict[key]
        else:
            mergedDict[key] = newDict[key]
    return mergedDict


def RemoveNonesFromDictData(originalDict: dict) -> dict:
    cleanDict = dict(originalDict)
    keysToPop = []
    for key in cleanDict:
        if isinstance(cleanDict[key], dict):
            cleanDict[key] = RemoveNonesFromDictData(cleanDict[key])
        else:
            if cleanDict[key] is None:
                keysToPop.append(key)
    for keyToPop in keysToPop:
        cleanDict.pop(keyToPop)
    return cleanDict


def RemoveMatchingDictData(originalDict: dict, matchingDict: dict) -> tuple[dict, dict]:
    changedData = {}
    cleanDict = dict(matchingDict)
    keysToPop = []
    for key in cleanDict:
        if key in originalDict:
            if isinstance(originalDict[key], dict) and isinstance(cleanDict[key], dict):
                data = RemoveMatchingDictData(originalDict[key], cleanDict[key])
                cleanDict[key] = data[0]
                changedData[key] = data[1]
            elif isinstance(originalDict[key], list) and isinstance(matchingDict[key], list):
                if CheckMatchingListData(originalDict[key], matchingDict[key]):
                    keysToPop.append(key)
                else:
                    changedData[key] = originalDict[key]
            else:
                if originalDict[key] == cleanDict[key]:
                    keysToPop.append(key)
                else:
                    changedData[key] = originalDict[key]
    for keyToPop in keysToPop:
        cleanDict.pop(keyToPop)
    return cleanDict, changedData


def CheckMatchingDictData(originalDict: dict, matchingDict: dict) -> bool:
    matching = True
    for key in matchingDict:
        if key in originalDict:
            if isinstance(originalDict[key], dict) and isinstance(matchingDict[key], dict):
                matching = CheckMatchingDictData(originalDict[key], matchingDict[key])
            elif isinstance(originalDict[key], list) and isinstance(matchingDict[key], list):
                matching = CheckMatchingListData(originalDict[key], matchingDict[key])
            else:
                matching = originalDict[key] == matchingDict[key]
        else:
            matching = False
        if not matching:
            break
    return matching


def CheckEmptyDictData(data: dict) -> bool:
    empty = True
    for key in data:
        if isinstance(data[key], dict):
            empty = CheckEmptyDictData(data[key])
        else:
            empty = False
        if not empty:
            break
    return empty


def CheckMatchingListData(originalList: list, matchingList: list) -> bool:
    matching = len(originalList) == len(matchingList)
    for i in range(len(originalList)):
        if not matching:
            break
        if isinstance(originalList[i], dict) and isinstance(matchingList[i], dict):
            matching = CheckMatchingDictData(originalList[i], matchingList[i])
        elif isinstance(originalList[i], list) and isinstance(matchingList[i], list):
            matching = CheckMatchingListData(originalList[i], matchingList[i])
        else:
            matching = originalList[i] == matchingList[i]
    return matching


def RemoveKeys(data: dict, keyMap: dict, ignoreKeys: list[str] | None = None) -> None:
    if ignoreKeys is None:
        ignoreKeys = ["id"]
    for key in keyMap:
        if key in data and key not in ignoreKeys:
            if isinstance(keyMap[key], dict):
                if isinstance(data[key], dict):
                    RemoveKeys(data[key], keyMap[key])
            else:
                data.pop(key)
