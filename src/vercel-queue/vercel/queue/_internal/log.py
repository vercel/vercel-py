from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import json
import logging
import os
import re
from collections.abc import Mapping
from urllib.parse import unquote, urlsplit, urlunsplit

if TYPE_CHECKING:
    from .types import Message, MessageMetadata

_LOGGER_NAME = "vercel.queue"
_REDACTED = "<redacted>"
_REDACTED_RECEIPT_HANDLE = "rh_1_REDACTED"
_DEBUG_ENV = "VERCEL_QUEUE_DEBUG"
_SENSITIVE_FIELD_NAMES = frozenset({
    "authorization",
    "body",
    "content",
    "payload",
    "raw_body",
    "receipt_handle",
    "receipthandle",
    "token",
})
_SENSITIVE_HEADER_NAMES = frozenset({
    "authorization",
    "cookie",
    "set-cookie",
    "x-vercel-oidc-token",
})
_BEARER_RE = re.compile(r"Bearer\s+[^\s,;]+")
_RECEIPT_RE = re.compile(r"rh_[^\s\"']+")


def debug_enabled() -> bool:
    return os.environ.get(_DEBUG_ENV) in {"1", "true"}


def configure_asgi_logger() -> None:
    logging.getLogger(_LOGGER_NAME).setLevel(logging.INFO if debug_enabled() else logging.WARNING)


def debug_log(event: str, **fields: Any) -> None:
    if not debug_enabled():
        return
    payload = {"event": event, **_redact_fields(fields)}
    logging.getLogger(_LOGGER_NAME).info(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    )


def _message_metadata(message: Message[Any] | MessageMetadata) -> MessageMetadata:
    return cast("MessageMetadata", getattr(message, "metadata", message))


def message_debug_fields(message: Message[Any] | MessageMetadata) -> dict[str, object]:
    metadata = _message_metadata(message)
    fields: dict[str, object] = {
        "message_id": metadata.message_id,
        "topic": metadata.topic,
        "consumer_group": str(metadata.consumer_group),
        "delivery_count": metadata.delivery_count,
    }
    if metadata.region is not None:
        fields["region"] = metadata.region
    return fields


def debug_log_for_msg(
    event: str,
    message: Message[Any] | MessageMetadata,
    **fields: Any,
) -> None:
    if not debug_enabled():
        return
    debug_log(event, **message_debug_fields(message), **fields)


def safe_header_names(headers: Mapping[Any, object]) -> list[str]:
    names = [str(name) for name in headers]
    return sorted(name for name in names if name.lower() not in _SENSITIVE_HEADER_NAMES)


def safe_url(url: str) -> str:
    parts = urlsplit(url)
    path_parts = parts.path.split("/")
    redacted_parts: list[str] = []
    redact_next = False
    for part in path_parts:
        if redact_next and part:
            redacted_parts.append(_REDACTED_RECEIPT_HANDLE)
            redact_next = False
            continue
        redacted_parts.append(part)
        redact_next = unquote(part) == "lease"
    return urlunsplit((parts.scheme, parts.netloc, "/".join(redacted_parts), "", ""))


def redact_text(value: str) -> str:
    return _RECEIPT_RE.sub(
        _REDACTED_RECEIPT_HANDLE,
        _BEARER_RE.sub(f"Bearer {_REDACTED}", value),
    )


def content_type(headers: Mapping[str, str]) -> str | None:
    for name, value in headers.items():
        if name.lower() == "content-type":
            return value
    return None


def redact_value(name: str, value: Any) -> Any:
    lower_name = name.lower()
    if lower_name in {"receipt_handle", "receipthandle"}:
        return _REDACTED_RECEIPT_HANDLE
    if lower_name in _SENSITIVE_FIELD_NAMES:
        return _REDACTED
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, Mapping):
        return {str(key): redact_value(str(key), item) for key, item in sorted(value.items())}
    if isinstance(value, tuple | list | set | frozenset):
        return [redact_value(name, item) for item in value]
    return _jsonable(value)


def _redact_fields(fields: Mapping[str, Any]) -> dict[str, Any]:
    return {name: redact_value(name, value) for name, value in fields.items() if value is not None}


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in sorted(value.items())}
    if isinstance(value, tuple | list | set | frozenset):
        return [_jsonable(item) for item in value]
    return str(value)
