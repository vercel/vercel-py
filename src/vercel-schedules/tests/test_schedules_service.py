"""Argument validation and request-body rendering, with no session or network.

`build_create_body`, `build_update_body`, and `resolve_one_off` are pure; the
public surface only forwards to them. Testing them directly keeps each case to
the inputs and the resulting dict.
"""

from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from vercel.schedules import MAX_JITTER, MIN_JITTER, SchedulesValidationError
from vercel.schedules._internal.sentinel import UNSET
from vercel.schedules._internal.service import (
    build_create_body,
    build_update_body,
    resolve_one_off,
)

AT = datetime(2026, 10, 1, 9, 30, tzinfo=timezone.utc)


def create_body(**overrides: Any) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "name": "tick",
        "topic": "t",
        "cron": None,
        "at": None,
        "timezone": None,
        "namespace": None,
        "jitter": None,
        "payload": UNSET,
    }
    kwargs.update(overrides)
    return build_create_body(**kwargs)


def update_body(**overrides: Any) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "cron": None,
        "at": None,
        "timezone": None,
        "topic": None,
        "jitter": UNSET,
        "payload": UNSET,
    }
    kwargs.update(overrides)
    return build_update_body(**kwargs)


# --- one-off resolution ----------------------------------------------------


def test_zoneinfo_datetime_supplies_the_timezone() -> None:
    local = datetime(2026, 10, 1, 9, 30, tzinfo=ZoneInfo("America/Los_Angeles"))

    assert resolve_one_off(local, None) == ("2026-10-01T09:30:00", "America/Los_Angeles")


@pytest.mark.parametrize(("second", "microsecond"), [(15, 0), (0, 1)])
def test_one_off_rejects_subminute_precision(second: int, microsecond: int) -> None:
    at = AT.replace(second=second, microsecond=microsecond)
    with pytest.raises(SchedulesValidationError, match="whole-minute precision"):
        resolve_one_off(at, None)
    with pytest.raises(SchedulesValidationError, match="whole-minute precision"):
        resolve_one_off(at.replace(tzinfo=None), "Europe/Berlin")
    with pytest.raises(SchedulesValidationError, match="whole-minute precision"):
        resolve_one_off(at, "Asia/Tokyo")


@pytest.mark.parametrize("tzinfo", [timezone.utc, ZoneInfo("UTC")])
def test_utc_datetime_resolves_to_utc(tzinfo: Any) -> None:
    assert resolve_one_off(AT.replace(tzinfo=tzinfo), None) == ("2026-10-01T09:30:00", "UTC")


def test_naive_datetime_uses_the_explicit_timezone_verbatim() -> None:
    assert resolve_one_off(datetime(2026, 10, 1, 9, 30), "Europe/Berlin") == (
        "2026-10-01T09:30:00",
        "Europe/Berlin",
    )


def test_aware_datetime_is_converted_into_the_explicit_timezone() -> None:
    assert resolve_one_off(AT, "Asia/Tokyo") == ("2026-10-01T18:30:00", "Asia/Tokyo")


# --- create ----------------------------------------------------------------


def test_create_renders_every_field() -> None:
    assert create_body(
        name="cleanup",
        topic="scheduled-cleanup",
        cron="0 * * * *",
        timezone="America/New_York",
        namespace="jobs",
        jitter=timedelta(minutes=2),
        payload={"maxAgeDays": 30},
    ) == {
        "name": "cleanup",
        "namespace": "jobs",
        "expression": {"type": "cron", "cron": "0 * * * *"},
        "timezone": "America/New_York",
        "jitter": 2,
        "target": {"type": "queue", "topic": "scheduled-cleanup"},
        "payload": {"maxAgeDays": 30},
    }


def test_create_omits_optional_fields() -> None:
    assert create_body(cron="* * * * *") == {
        "name": "tick",
        "expression": {"type": "cron", "cron": "* * * * *"},
        "target": {"type": "queue", "topic": "t"},
    }


def test_create_one_off_always_carries_a_timezone() -> None:
    assert create_body(at=AT) == {
        "name": "tick",
        "expression": {"type": "single", "at": "2026-10-01T09:30:00"},
        "timezone": "UTC",
        "target": {"type": "queue", "topic": "t"},
    }


def test_create_preserves_explicit_json_null_payload() -> None:
    assert create_body(cron="* * * * *", payload=None)["payload"] is None


@pytest.mark.parametrize("jitter", [MIN_JITTER, timedelta(minutes=7), MAX_JITTER])
def test_create_accepts_jitter_bounds_inclusive(jitter: timedelta) -> None:
    assert create_body(cron="* * * * *", jitter=jitter)["jitter"] == jitter // timedelta(minutes=1)


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({}, "exactly one of cron or at"),
        ({"cron": "* * * * *", "at": AT}, "exactly one of cron or at"),
        ({"cron": "  "}, "cron must be"),
        ({"cron": "* * * * *", "timezone": ""}, "timezone must be"),
        ({"at": AT.replace(tzinfo=None)}, "at is naive"),
        ({"at": AT.astimezone(timezone(timedelta(hours=-5)))}, "fixed offset"),
        ({"at": AT, "timezone": "Mars/Olympus_Mons"}, "not a known IANA timezone"),
        ({"at": AT.replace(second=15)}, "whole-minute precision"),
        ({"cron": "* * * * *", "jitter": timedelta(seconds=-1)}, "between"),
        ({"cron": "* * * * *", "jitter": timedelta(seconds=59)}, "between"),
        ({"cron": "* * * * *", "jitter": timedelta(minutes=15, seconds=1)}, "between"),
        ({"cron": "* * * * *", "jitter": timedelta(minutes=1, seconds=1)}, "whole number"),
        ({"cron": "* * * * *", "name": ""}, "name"),
        ({"cron": "* * * * *", "topic": ""}, "topic"),
        ({"cron": "* * * * *", "namespace": ""}, "namespace"),
    ],
)
def test_create_rejects_bad_arguments(kwargs: dict[str, Any], match: str) -> None:
    with pytest.raises(SchedulesValidationError, match=match):
        create_body(**kwargs)


# --- update ----------------------------------------------------------------


def test_update_renders_only_supplied_fields() -> None:
    assert update_body(
        cron="0 2 * * *",
        timezone="America/New_York",
        topic="daily-cleanup",
        jitter=None,
        payload={"job": "cleanup"},
    ) == {
        "expression": {"type": "cron", "cron": "0 2 * * *"},
        "timezone": "America/New_York",
        "target": {"type": "queue", "topic": "daily-cleanup"},
        "jitter": None,
        "payload": {"job": "cleanup"},
    }


def test_update_jitter_alone() -> None:
    assert update_body(jitter=timedelta(minutes=5)) == {"jitter": 5}


def test_update_timezone_alone() -> None:
    assert update_body(timezone="Asia/Tokyo") == {"timezone": "Asia/Tokyo"}


def test_update_cron_without_timezone_leaves_timezone_out() -> None:
    assert update_body(cron="0 2 * * *") == {"expression": {"type": "cron", "cron": "0 2 * * *"}}


def test_update_one_off_derives_timezone() -> None:
    assert update_body(at=datetime(2026, 12, 1, 8, tzinfo=ZoneInfo("Asia/Tokyo"))) == {
        "expression": {"type": "single", "at": "2026-12-01T08:00:00"},
        "timezone": "Asia/Tokyo",
    }


def test_update_one_off_converts_into_explicit_timezone() -> None:
    assert update_body(at=AT, timezone="Asia/Tokyo") == {
        "expression": {"type": "single", "at": "2026-10-01T18:30:00"},
        "timezone": "Asia/Tokyo",
    }


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({}, "at least one field"),
        ({"cron": "* * * * *", "at": AT}, "at most one of cron or at"),
        ({"at": AT.replace(microsecond=1)}, "whole-minute precision"),
        ({"jitter": timedelta(seconds=30)}, "between"),
        ({"topic": ""}, "topic"),
    ],
)
def test_update_rejects_bad_arguments(kwargs: dict[str, Any], match: str) -> None:
    with pytest.raises(SchedulesValidationError, match=match):
        update_body(**kwargs)


def test_validation_error_is_a_value_error() -> None:
    assert issubclass(SchedulesValidationError, ValueError)
