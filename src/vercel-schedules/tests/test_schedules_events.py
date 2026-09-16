"""Parsing of inbound schedule dispatches, ported from the TypeScript suite."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, Any

import pytest
from conftest import DISPATCH_HEADERS
from pydantic import BaseModel

from vercel.schedules import (
    ScheduleEvent,
    ScheduleEventParseError,
    parse_schedule_event,
    resolve_payload_type,
)

JSON = {"content-type": "application/json"}


def test_parses_metadata_and_payload() -> None:
    event = parse_schedule_event({**DISPATCH_HEADERS, **JSON}, b'{"maxAgeDays": 30}')

    assert isinstance(event, ScheduleEvent)
    assert event.schedule_id == "01JABCDEF0123456789012345"
    assert event.name == "every-ten"
    assert event.namespace == "default"
    assert event.source == "static"
    assert event.fired_at == datetime(2026, 9, 2, 12, tzinfo=timezone.utc)
    assert event.payload == {"maxAgeDays": 30}


def test_header_names_are_case_insensitive() -> None:
    headers = {name.upper(): value for name, value in DISPATCH_HEADERS.items()}

    assert parse_schedule_event(headers).name == "every-ten"


@pytest.mark.parametrize("body", [None, b"", ""])
@pytest.mark.parametrize("headers", [DISPATCH_HEADERS, {**DISPATCH_HEADERS, **JSON}])
def test_missing_body_means_no_payload(headers: dict[str, str], body: bytes | str | None) -> None:
    assert parse_schedule_event(headers, body).payload is None


@pytest.mark.parametrize(("raw", "expected"), [(b"null", None), (b"false", False), (b"0", 0)])
def test_falsy_json_payloads_are_preserved(raw: bytes, expected: object) -> None:
    assert parse_schedule_event({**DISPATCH_HEADERS, **JSON}, raw).payload == expected


def test_payload_type_validates_and_constructs() -> None:
    class Cleanup(BaseModel):
        max_age_days: int

    event = parse_schedule_event(
        {**DISPATCH_HEADERS, **JSON}, b'{"max_age_days": "7"}', payload_type=Cleanup
    )

    assert event.payload == Cleanup(max_age_days=7)


def test_payload_type_mismatch_is_a_parse_error() -> None:
    class Cleanup(BaseModel):
        max_age_days: int

    with pytest.raises(ScheduleEventParseError, match="Cleanup"):
        parse_schedule_event(
            {**DISPATCH_HEADERS, **JSON}, b'{"max_age_days": "x"}', payload_type=Cleanup
        )


def test_payload_type_is_skipped_when_there_is_no_payload() -> None:
    class Cleanup(BaseModel):
        max_age_days: int

    assert parse_schedule_event(DISPATCH_HEADERS, payload_type=Cleanup).payload is None


def test_resolves_payload_type_from_handler_annotation() -> None:
    class Cleanup(BaseModel):
        max_age_days: int

    async def handler(event: ScheduleEvent[Cleanup]) -> None:
        pass

    assert resolve_payload_type(handler) is Cleanup


def test_rejects_event_subclass_annotation() -> None:
    class Cleanup(BaseModel):
        max_age_days: int

    class CleanupEvent(ScheduleEvent[Cleanup]):
        pass

    async def handler(event: CleanupEvent) -> None:
        pass

    with pytest.raises(TypeError, match="annotated directly"):
        resolve_payload_type(handler)


def test_resolves_annotated_event_with_unhashable_metadata() -> None:
    class Cleanup(BaseModel):
        max_age_days: int

    async def handler(
        event: Annotated[ScheduleEvent[Cleanup], {"source": "runtime"}],
    ) -> None:
        pass

    assert resolve_payload_type(handler) is Cleanup


async def _unannotated(event) -> None:  # type: ignore[no-untyped-def]  # noqa: ANN001
    pass


async def _bare(event: ScheduleEvent) -> None:
    pass


async def _any(event: ScheduleEvent[Any]) -> None:
    pass


@pytest.mark.parametrize("handler", [_unannotated, _bare, _any])
def test_untyped_handler_payload_resolves_to_none(handler: Any) -> None:
    assert resolve_payload_type(handler) is None


def test_rejects_non_event_handler_annotation() -> None:
    async def handler(event: str) -> None:
        pass

    with pytest.raises(TypeError, match=r"ScheduleEvent\[T\]"):
        resolve_payload_type(handler)  # type: ignore[arg-type]


def test_rejects_handler_with_wrong_signature() -> None:
    async def handler() -> None:
        pass

    with pytest.raises(TypeError, match="exactly one event parameter"):
        resolve_payload_type(handler)  # type: ignore[arg-type]


@pytest.mark.parametrize("method", ["GET", "put"])
def test_rejects_non_post(method: str) -> None:
    with pytest.raises(ScheduleEventParseError, match="POST requests"):
        parse_schedule_event(DISPATCH_HEADERS, method=method)


def test_rejects_wrong_spec_version() -> None:
    with pytest.raises(ScheduleEventParseError, match="spec version: 0.3"):
        parse_schedule_event({**DISPATCH_HEADERS, "ce-specversion": "0.3"})


def test_rejects_wrong_event_type() -> None:
    with pytest.raises(ScheduleEventParseError, match="CloudEvent type: com.example.other"):
        parse_schedule_event({**DISPATCH_HEADERS, "ce-type": "com.example.other"})


@pytest.mark.parametrize(
    "name",
    [
        "ce-source",
        "ce-id",
        "ce-time",
        "ce-vssscheduleid",
        "ce-vssschedulename",
        "ce-vssnamespace",
        "ce-vssschedulesource",
    ],
)
def test_rejects_missing_or_empty_required_header(name: str) -> None:
    without = {k: v for k, v in DISPATCH_HEADERS.items() if k != name}
    with pytest.raises(ScheduleEventParseError, match=f"missing the {name} header"):
        parse_schedule_event(without)
    with pytest.raises(ScheduleEventParseError, match=f"missing the {name} header"):
        parse_schedule_event({**DISPATCH_HEADERS, name: ""})


def test_rejects_invalid_timestamp() -> None:
    with pytest.raises(ScheduleEventParseError, match="not a valid timestamp"):
        parse_schedule_event({**DISPATCH_HEADERS, "ce-time": "yesterday"})


@pytest.mark.parametrize("value", ["2026-09-02", "2026-09-02T12:00:00"])
def test_rejects_timestamp_without_timezone(value: str) -> None:
    with pytest.raises(ScheduleEventParseError, match="timezone offset"):
        parse_schedule_event({**DISPATCH_HEADERS, "ce-time": value})


def test_rejects_non_json_content_type() -> None:
    with pytest.raises(ScheduleEventParseError, match="application/json"):
        parse_schedule_event({**DISPATCH_HEADERS, "content-type": "text/plain"}, b"hi")


def test_accepts_content_type_parameters() -> None:
    headers = {**DISPATCH_HEADERS, "content-type": "Application/JSON; charset=utf-8"}

    assert parse_schedule_event(headers, b"[1]").payload == [1]


def test_rejects_invalid_json() -> None:
    with pytest.raises(ScheduleEventParseError, match="not valid JSON"):
        parse_schedule_event({**DISPATCH_HEADERS, **JSON}, b"{")


def test_parse_error_is_a_value_error() -> None:
    assert issubclass(ScheduleEventParseError, ValueError)
