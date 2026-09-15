"""Parsing of inbound schedule dispatches.

A schedule fires as a binary-mode CloudEvent: the event attributes travel as
`ce-*` request headers and the configured payload, if any, is the JSON body.
"""

import json
from collections.abc import Mapping
from datetime import datetime
from typing import Any, TypeAlias, TypeVar, overload

from pydantic import TypeAdapter, ValidationError

from vercel.schedules._internal.errors import ScheduleEventParseError
from vercel.schedules._internal.models import ScheduleEvent

SCHEDULE_CLOUD_EVENT_TYPE = "com.vercel.schedule.v1beta"
"""CloudEvent type used for schedule function dispatches."""

CLOUD_EVENT_SPEC_VERSION_HEADER = "ce-specversion"
CLOUD_EVENT_TYPE_HEADER = "ce-type"
CLOUD_EVENT_SOURCE_HEADER = "ce-source"
CLOUD_EVENT_ID_HEADER = "ce-id"
SCHEDULE_FIRED_AT_HEADER = "ce-time"
SCHEDULE_ID_HEADER = "ce-vssscheduleid"
SCHEDULE_NAME_HEADER = "ce-vssschedulename"
SCHEDULE_NAMESPACE_HEADER = "ce-vssnamespace"
SCHEDULE_SOURCE_HEADER = "ce-vssschedulesource"

_SUPPORTED_SPEC_VERSION = "1.0"

T = TypeVar("T")

RequestBody: TypeAlias = bytes | bytearray | memoryview | str | None
"""A fully-read request body, or `None` when the request carried none."""


def _lower_headers(headers: Mapping[str, str]) -> dict[str, str]:
    return {name.lower(): value for name, value in headers.items()}


def _require(headers: Mapping[str, str], name: str) -> str:
    value = headers.get(name)
    if not value:
        raise ScheduleEventParseError(
            f"Request is missing the {name} header; it does not look like a schedule dispatch"
        )
    return value


def _parse_fired_at(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ScheduleEventParseError(
            f"Schedule dispatch time is not a valid timestamp: {value}"
        ) from exc


def _media_type(content_type: str | None) -> str | None:
    if content_type is None:
        return None
    return content_type.split(";", 1)[0].strip().lower()


def _decode_payload(headers: Mapping[str, str], body: RequestBody) -> tuple[bool, Any]:
    """Return `(present, value)`, so a JSON `null` payload stays distinguishable."""
    if body is None:
        return False, None
    raw = body.encode() if isinstance(body, str) else bytes(body)
    content_type = headers.get("content-type")
    if content_type is None and not raw:
        return False, None
    if _media_type(content_type) != "application/json":
        raise ScheduleEventParseError(
            "Schedule dispatch payload must use the application/json content type"
        )
    try:
        return True, json.loads(raw)
    except ValueError as exc:
        raise ScheduleEventParseError("Schedule dispatch payload is not valid JSON") from exc


@overload
def parse_schedule_event(
    headers: Mapping[str, str],
    body: RequestBody = None,
    *,
    method: str = "POST",
    payload_type: None = None,
) -> ScheduleEvent[Any]: ...


@overload
def parse_schedule_event(
    headers: Mapping[str, str],
    body: RequestBody = None,
    *,
    method: str = "POST",
    payload_type: type[T],
) -> ScheduleEvent[T]: ...


def parse_schedule_event(
    headers: Mapping[str, str],
    body: RequestBody = None,
    *,
    method: str = "POST",
    payload_type: type[T] | None = None,
) -> ScheduleEvent[Any]:
    """Parse a schedule dispatch from its request headers and body.

    Framework-agnostic: pass whatever headers mapping and fully-read body your
    server gives you. Header names are matched case-insensitively.

    Args:
        headers: Request headers.
        body: Request body, already read. `None` or empty means no payload.
        method: Request method. Dispatches are always `POST`.
        payload_type: Validate the payload against this type (a pydantic model,
            dataclass, `TypedDict`, or any type pydantic can build an adapter
            for). Without it the payload is the decoded JSON as-is.

    Returns:
        The parsed event, with the schedule metadata and payload.

    Raises:
        ScheduleEventParseError: If the request is not a schedule dispatch, or
            its payload is not JSON or does not match `payload_type`.
    """
    if method.upper() != "POST":
        raise ScheduleEventParseError(f"Schedule dispatches are POST requests, received {method}")

    lowered = _lower_headers(headers)

    spec_version = lowered.get(CLOUD_EVENT_SPEC_VERSION_HEADER)
    if spec_version != _SUPPORTED_SPEC_VERSION:
        raise ScheduleEventParseError(
            f"Unsupported CloudEvent spec version: {spec_version or '<missing>'}"
        )
    event_type = lowered.get(CLOUD_EVENT_TYPE_HEADER)
    if event_type != SCHEDULE_CLOUD_EVENT_TYPE:
        raise ScheduleEventParseError(
            f"Unsupported schedule CloudEvent type: {event_type or '<missing>'}"
        )
    _require(lowered, CLOUD_EVENT_SOURCE_HEADER)
    _require(lowered, CLOUD_EVENT_ID_HEADER)
    fired_at = _parse_fired_at(_require(lowered, SCHEDULE_FIRED_AT_HEADER))
    schedule_id = _require(lowered, SCHEDULE_ID_HEADER)
    name = _require(lowered, SCHEDULE_NAME_HEADER)
    namespace = _require(lowered, SCHEDULE_NAMESPACE_HEADER)
    source = _require(lowered, SCHEDULE_SOURCE_HEADER)

    present, payload = _decode_payload(lowered, body)
    if present and payload_type is not None:
        try:
            payload = TypeAdapter(payload_type).validate_python(payload)
        except ValidationError as exc:
            raise ScheduleEventParseError(
                f"Schedule dispatch payload does not match {payload_type.__name__}: {exc}"
            ) from exc

    return ScheduleEvent(
        schedule_id=schedule_id,
        name=name,
        namespace=namespace,
        fired_at=fired_at,
        source=source,
        payload=payload,
    )


__all__ = [
    "CLOUD_EVENT_ID_HEADER",
    "CLOUD_EVENT_SOURCE_HEADER",
    "CLOUD_EVENT_SPEC_VERSION_HEADER",
    "CLOUD_EVENT_TYPE_HEADER",
    "SCHEDULE_CLOUD_EVENT_TYPE",
    "SCHEDULE_FIRED_AT_HEADER",
    "SCHEDULE_ID_HEADER",
    "SCHEDULE_NAMESPACE_HEADER",
    "SCHEDULE_NAME_HEADER",
    "SCHEDULE_SOURCE_HEADER",
    "RequestBody",
    "parse_schedule_event",
]
