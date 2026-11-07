import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from vercel.schedules import NotAScheduleInvocationError, ScheduleEventParseError, ScheduleMetadata

FIRING_ENV = {
    "VERCEL_SCHEDULE_ID": "schedule-id",
    "VERCEL_SCHEDULE_EXECUTION_ID": "execution-id",
    "VERCEL_SCHEDULE_NAME": "nightly",
    "VERCEL_SCHEDULE_NAMESPACE": "default",
    "VERCEL_SCHEDULE_SCHEDULED_AT": "2026-09-02T12:00:00.000Z",
    "VERCEL_SCHEDULE_SOURCE": "static",
    "VERCEL_SCHEDULE_EXPRESSION": "0 12 * * *",
}

METADATA = ScheduleMetadata(
    schedule_id="schedule-id",
    execution_id="execution-id",
    name="nightly",
    namespace="default",
    scheduled_at=datetime(2026, 9, 2, 12, tzinfo=timezone.utc),
    source="static",
    expression="0 12 * * *",
)


def test_reads_the_firing_from_the_process_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in FIRING_ENV.items():
        monkeypatch.setenv(name, value)

    assert ScheduleMetadata.from_env() == METADATA


def test_a_process_with_only_client_settings_is_not_a_firing() -> None:
    # Schedule client settings share the prefix and outlive a firing.
    env = {"VERCEL_SCHEDULE_BASE_URL": "https://schedules.test"}

    with pytest.raises(NotAScheduleInvocationError):
        ScheduleMetadata.from_env(env)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"VERCEL_SCHEDULE_NAME": ""}, "VERCEL_SCHEDULE_NAME is not set"),
        ({"VERCEL_SCHEDULE_SCHEDULED_AT": "noon"}, "invalid VERCEL_SCHEDULE_SCHEDULED_AT"),
        ({"VERCEL_SCHEDULE_SCHEDULED_AT": "2026-09-02T12:00:00"}, "missing offset"),
    ],
)
def test_rejects_an_invalid_firing(overrides: dict[str, str], message: str) -> None:
    with pytest.raises(ScheduleEventParseError, match=message) as exc_info:
        ScheduleMetadata.from_env({**FIRING_ENV, **overrides})

    assert not isinstance(exc_info.value, NotAScheduleInvocationError)


# The schedule dispatch protocol's conformance fixtures, copied from
# vercel/schedules docs/fixtures/schedule-dispatch-v1beta.json.
DISPATCH_FIXTURES = json.loads(
    (Path(__file__).parent / "fixtures" / "schedule-dispatch-v1beta.json").read_text()
)


@pytest.mark.parametrize("case", DISPATCH_FIXTURES["accept"], ids=lambda case: case["description"])
def test_reads_a_conforming_dispatch(case: dict[str, Any]) -> None:
    expected = case["expected"]

    assert ScheduleMetadata.from_headers(case["headers"]) == ScheduleMetadata(
        schedule_id=expected["scheduleId"],
        execution_id=expected["executionId"],
        name=expected["name"],
        namespace=expected["namespace"],
        scheduled_at=datetime.fromisoformat(expected["scheduledAt"].replace("Z", "+00:00")),
        source=expected["source"],
        expression=expected["expression"],
    )


@pytest.mark.parametrize("case", DISPATCH_FIXTURES["reject"], ids=lambda case: case["description"])
def test_rejects_a_nonconforming_dispatch(case: dict[str, Any]) -> None:
    with pytest.raises(ScheduleEventParseError):
        ScheduleMetadata.from_headers(case["headers"])


def test_a_request_without_a_cloudevent_type_is_not_a_firing() -> None:
    # A public path also receives direct requests and legacy Cron Jobs.
    with pytest.raises(NotAScheduleInvocationError):
        ScheduleMetadata.from_headers({"user-agent": "vercel-cron/1.0"})


def test_header_names_are_case_insensitive() -> None:
    [case] = [c for c in DISPATCH_FIXTURES["accept"] if c["headers"]["ce-id"] == "execution-1"]
    headers = {name.title(): value for name, value in case["headers"].items()}

    assert ScheduleMetadata.from_headers(headers).execution_id == "execution-1"
