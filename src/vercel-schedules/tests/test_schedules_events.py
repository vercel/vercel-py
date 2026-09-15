"""Parsing of inbound schedule dispatches, ported from the TypeScript suite."""

from datetime import datetime, timezone

import pytest
from conftest import DISPATCH_HEADERS
from pydantic import BaseModel

from vercel.schedules import (
    SCHEDULE_CLOUD_EVENT_TYPE,
    ScheduleEvent,
    ScheduleEventParseError,
    parse_schedule_event,
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
def test_missing_body_means_no_payload(body: bytes | str | None) -> None:
    assert parse_schedule_event(DISPATCH_HEADERS, body).payload is None


def test_empty_json_body_means_no_payload() -> None:
    assert parse_schedule_event({**DISPATCH_HEADERS, **JSON}, b"").payload is None


@pytest.mark.parametrize("raw", [b"null", b"false", b"0"])
def test_falsy_json_payloads_are_preserved(raw: bytes) -> None:
    event = parse_schedule_event({**DISPATCH_HEADERS, **JSON}, raw)

    assert event.payload == (None if raw == b"null" else (False if raw == b"false" else 0))


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


@pytest.mark.parametrize("method", ["GET", "put"])
def test_rejects_non_post(method: str) -> None:
    with pytest.raises(ScheduleEventParseError, match="POST requests"):
        parse_schedule_event(DISPATCH_HEADERS, method=method)


def test_rejects_wrong_spec_version() -> None:
    with pytest.raises(ScheduleEventParseError, match="spec version: 0.3"):
        parse_schedule_event({**DISPATCH_HEADERS, "ce-specversion": "0.3"})


def test_rejects_wrong_event_type() -> None:
    assert SCHEDULE_CLOUD_EVENT_TYPE == "com.vercel.schedule.v1beta"
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
    with pytest.raises(ValueError):
        parse_schedule_event({})
