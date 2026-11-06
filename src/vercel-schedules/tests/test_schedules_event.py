from datetime import datetime, timezone

import pytest

from vercel.schedules import NotAScheduleInvocationError, ScheduleEvent, ScheduleEventError

FIRING_ENV = {
    "VERCEL_SCHEDULE_ID": "schedule-id",
    "VERCEL_SCHEDULE_EXECUTION_ID": "execution-id",
    "VERCEL_SCHEDULE_NAME": "nightly",
    "VERCEL_SCHEDULE_NAMESPACE": "default",
    "VERCEL_SCHEDULE_SCHEDULED_AT": "2026-09-02T12:00:00.000Z",
    "VERCEL_SCHEDULE_SOURCE": "static",
    "VERCEL_SCHEDULE_EXPRESSION": "0 12 * * *",
}

EVENT = ScheduleEvent(
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

    assert ScheduleEvent.from_env() == EVENT


def test_a_process_with_only_client_settings_is_not_a_firing() -> None:
    # Schedule client settings share the prefix and outlive a firing.
    env = {"VERCEL_SCHEDULE_BASE_URL": "https://schedules.test"}

    with pytest.raises(NotAScheduleInvocationError):
        ScheduleEvent.from_env(env)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"VERCEL_SCHEDULE_NAME": ""}, "VERCEL_SCHEDULE_NAME is not set"),
        ({"VERCEL_SCHEDULE_SCHEDULED_AT": "noon"}, "invalid VERCEL_SCHEDULE_SCHEDULED_AT"),
        ({"VERCEL_SCHEDULE_SCHEDULED_AT": "2026-09-02T12:00:00"}, "missing offset"),
    ],
)
def test_rejects_an_invalid_firing(overrides: dict[str, str], message: str) -> None:
    with pytest.raises(ScheduleEventError, match=message) as exc_info:
        ScheduleEvent.from_env({**FIRING_ENV, **overrides})

    assert not isinstance(exc_info.value, NotAScheduleInvocationError)
