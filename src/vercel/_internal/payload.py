from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, TypeAlias, cast

JSONScalar: TypeAlias = str | int | float | bool | None
JSONValue: TypeAlias = JSONScalar | dict[str, "JSONValue"] | list["JSONValue"]

_FIRST_UNDERSCORE = re.compile(r"_([a-zA-Z])")


class PayloadConvertible(Protocol):
    def to_payload(self) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class RawPayload:
    """Opaque payload subtree that is already in wire format."""

    value: Any


def to_camel_case(key: str) -> str:
    if not key or key.startswith("_"):
        return key
    return _FIRST_UNDERSCORE.sub(lambda match: match.group(1).upper(), key)


def marshal_payload(value: Any) -> JSONValue:
    if isinstance(value, RawPayload):
        return cast(JSONValue, value.value)
    if hasattr(value, "to_payload"):
        return marshal_payload(cast(PayloadConvertible, value).to_payload())
    if isinstance(value, Mapping):
        payload: dict[str, JSONValue] = {}
        for key, nested_value in value.items():
            payload[to_camel_case(str(key))] = marshal_payload(nested_value)
        return payload
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [marshal_payload(item) for item in value]
    return cast(JSONValue, value)
