from __future__ import annotations

import json
from typing import Any


def _default_encoder(message: Any) -> Any:
    """Encode a message into a type publishable by Redis."""

    if isinstance(message, (bytes, bytearray)):
        return bytes(message)
    if isinstance(message, str):
        return message
    return json.dumps(message)


def _default_decoder(payload: Any) -> Any:
    """Decode a Redis pub/sub payload back into Python objects."""

    if isinstance(payload, (bytes, bytearray)):
        try:
            payload = payload.decode("utf-8")
        except UnicodeDecodeError:
            return bytes(payload)

    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return payload

    return payload
