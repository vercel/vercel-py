"""Argument validation and the decisions the service makes before a request.

Each case calls the sync surface over a mocked transport and checks the request
body it sends, or the error it raises before sending anything. Wire rendering
itself is covered in `test_schedules_api_client`.
"""

import json
from datetime import UTC, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import httpx2 as httpx
import pytest
from conftest import SCHEDULE_JSON, TEST_BASE_URL, sync_session_options

import vendor.respx as respx
from vercel.api import session
from vercel.schedules import (
    MAX_JITTER,
    MIN_JITTER,
    Cron,
    SchedulesValidationError,
    sync as schedules_sync,
)

AT = datetime(2026, 10, 1, 9, 30, tzinfo=UTC)
SCHEDULES_URL = f"{TEST_BASE_URL}/v1/schedules"


def sent_body(method: str, url: str, call: Any) -> dict[str, Any]:
    with respx.mock(assert_all_called=False) as router:
        route = router.request(method, url).mock(
            return_value=httpx.Response(200, json=SCHEDULE_JSON)
        )
        with session(service_options=sync_session_options()):
            call()
        return json.loads(route.calls.last.request.content)


def create_body(**overrides: Any) -> dict[str, Any]:
    kwargs: dict[str, Any] = {"name": "tick", "topic": "t", **overrides}
    return sent_body("POST", SCHEDULES_URL, lambda: schedules_sync.create_schedule(**kwargs))


def update_body(**overrides: Any) -> dict[str, Any]:
    return sent_body(
        "PATCH",
        f"{SCHEDULES_URL}/cleanup",
        lambda: schedules_sync.update_schedule("cleanup", **overrides),
    )


def one_off(at: datetime, timezone: str | None = None) -> tuple[str, str]:
    body = create_body(at=at, timezone=timezone)
    return body["expression"]["at"], body["timezone"]


# --- one-off resolution ----------------------------------------------------


def test_zoneinfo_datetime_supplies_the_timezone() -> None:
    local = datetime(2026, 10, 1, 9, 30, tzinfo=ZoneInfo("America/Los_Angeles"))

    assert one_off(local) == ("2026-10-01T09:30:00", "America/Los_Angeles")


@pytest.mark.parametrize(("second", "microsecond"), [(15, 0), (0, 1)])
def test_one_off_rounds_subminute_times_up(second: int, microsecond: int) -> None:
    # `datetime.now() + timedelta(hours=1)` always has seconds. Rounding up keeps
    # the firing from running before `at`.
    at = AT.replace(second=second, microsecond=microsecond)

    assert one_off(at) == ("2026-10-01T09:31:00", "UTC")
    assert one_off(at.replace(tzinfo=None), "Europe/Berlin") == (
        "2026-10-01T09:31:00",
        "Europe/Berlin",
    )
    assert one_off(at, "Asia/Tokyo") == ("2026-10-01T18:31:00", "Asia/Tokyo")


def test_one_off_rounding_carries_into_the_next_day() -> None:
    at = datetime(2026, 12, 31, 23, 59, 30, tzinfo=UTC)

    assert one_off(at) == ("2027-01-01T00:00:00", "UTC")


@pytest.mark.parametrize("tzinfo", [UTC, ZoneInfo("UTC")])
def test_utc_datetime_resolves_to_utc(tzinfo: Any) -> None:
    assert one_off(AT.replace(tzinfo=tzinfo)) == ("2026-10-01T09:30:00", "UTC")


def test_naive_datetime_uses_the_explicit_timezone_verbatim() -> None:
    assert one_off(datetime(2026, 10, 1, 9, 30), "Europe/Berlin") == (
        "2026-10-01T09:30:00",
        "Europe/Berlin",
    )


def test_aware_datetime_is_converted_into_the_explicit_timezone() -> None:
    assert one_off(AT, "Asia/Tokyo") == ("2026-10-01T18:30:00", "Asia/Tokyo")


# --- create ----------------------------------------------------------------


@pytest.mark.parametrize("cron", [Cron(hour=3), "0 3 * * *", "  0  3 * *   * "])
def test_create_accepts_a_cron_or_a_crontab_string(cron: Cron | str) -> None:
    assert create_body(cron=cron)["expression"] == {"type": "cron", "cron": "0 3 * * *"}


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
        ({"cron": "  "}, "must have 5 fields"),
        ({"cron": "0 3 * *"}, "must have 5 fields"),
        ({"cron": "0 3 * * MON"}, "day_of_week must hold only numbers"),
        ({"cron": "* * * * *", "timezone": ""}, "timezone must be"),
        ({"at": AT.replace(tzinfo=None)}, "at is naive"),
        ({"at": AT.replace(tzinfo=None), "timezone": " "}, "timezone must be"),
        ({"at": AT.astimezone(timezone(timedelta(hours=-5)))}, "fixed offset"),
        ({"at": AT, "timezone": "Mars/Olympus_Mons"}, "not a known IANA timezone"),
        ({"cron": "* * * * *", "jitter": timedelta(seconds=-1)}, "between"),
        ({"cron": "* * * * *", "jitter": timedelta(seconds=59)}, "between"),
        ({"cron": "* * * * *", "jitter": timedelta(minutes=15, seconds=1)}, "between"),
        ({"cron": "* * * * *", "jitter": timedelta(minutes=1, seconds=1)}, "whole number"),
        ({"cron": "* * * * *", "jitter": 5}, "must be a timedelta"),
        ({"cron": "* * * * *", "name": ""}, "name"),
        ({"cron": "* * * * *", "name": "."}, "name"),
        ({"cron": "* * * * *", "name": ".."}, "name"),
        ({"cron": "* * * * *", "topic": ""}, "topic"),
        ({"cron": "* * * * *", "namespace": ""}, "namespace"),
    ],
)
def test_create_rejects_bad_arguments(kwargs: dict[str, Any], match: str) -> None:
    with pytest.raises(SchedulesValidationError, match=match):
        create_body(**kwargs)


# --- update ----------------------------------------------------------------


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
        ({"jitter": timedelta(seconds=30)}, "between"),
        ({"topic": ""}, "topic"),
        ({"timezone": ""}, "timezone must be"),
    ],
)
def test_update_rejects_bad_arguments(kwargs: dict[str, Any], match: str) -> None:
    with pytest.raises(SchedulesValidationError, match=match):
        update_body(**kwargs)


def test_validation_error_is_a_value_error() -> None:
    assert issubclass(SchedulesValidationError, ValueError)
