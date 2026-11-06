import threading
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from vercel.schedules import (
    Cron,
    RegisteredSchedule,
    ScheduleEventParseError,
    ScheduleMetadata,
    SchedulesValidationError,
    get_registered_schedules,
    schedule,
)
from vercel.schedules._internal import registry


@pytest.fixture(autouse=True)
def empty_registry(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(registry, "_schedules", {})
    yield


def metadata(name: str) -> ScheduleMetadata:
    return ScheduleMetadata(
        schedule_id="schedule-id",
        execution_id="execution-id",
        name=name,
        namespace="default",
        scheduled_at=datetime(2026, 9, 2, 3, tzinfo=timezone.utc),
        source="static",
        expression="0 3 * * *",
    )


def test_registers_the_function_under_its_name() -> None:
    def reindex() -> None: ...

    assert schedule("0 3 * * *")(reindex) is reindex
    assert get_registered_schedules() == (
        RegisteredSchedule(
            name="reindex", cron=Cron(hour=3), timezone=None, jitter=None, func=reindex
        ),
    )


def test_overrides_name_timezone_and_jitter() -> None:
    @schedule(
        Cron(minute="*/15"),
        name="sync-billing",
        timezone="America/New_York",
        jitter=timedelta(minutes=2),
    )
    def sync() -> None: ...

    [registered] = get_registered_schedules()
    assert (registered.name, str(registered.cron), registered.timezone, registered.jitter) == (
        "sync-billing",
        "*/15 * * * *",
        "America/New_York",
        timedelta(minutes=2),
    )


async def test_runs_a_sync_handler_off_the_event_loop() -> None:
    threads: list[threading.Thread] = []

    @schedule("0 3 * * *")
    def reindex() -> None:
        threads.append(threading.current_thread())

    [registered] = get_registered_schedules()
    await registered.run(metadata("reindex"))

    assert threads and threads[0] is not threading.current_thread()


async def test_passes_the_metadata_to_an_async_handler_that_takes_it() -> None:
    received: list[ScheduleMetadata] = []

    @schedule("0 3 * * *")
    async def reindex(firing: ScheduleMetadata) -> None:
        received.append(firing)

    [registered] = get_registered_schedules()
    await registered.run(metadata("reindex"))

    assert received == [metadata("reindex")]


async def test_handler_errors_propagate() -> None:
    @schedule("0 3 * * *")
    async def reindex() -> None:
        raise RuntimeError("index unavailable")

    [registered] = get_registered_schedules()
    with pytest.raises(RuntimeError, match="index unavailable"):
        await registered.run(metadata("reindex"))


async def test_rejects_a_firing_of_another_schedule() -> None:
    @schedule("0 3 * * *")
    async def reindex() -> None: ...

    [registered] = get_registered_schedules()
    with pytest.raises(ScheduleEventParseError, match="cannot run schedule 'reindex'"):
        await registered.run(metadata("cleanup"))


def test_rejects_a_second_handler_with_the_same_name() -> None:
    @schedule("0 3 * * *", name="nightly")
    def reindex() -> None: ...

    def cleanup() -> None: ...

    with pytest.raises(SchedulesValidationError, match="already declared by"):
        schedule("0 4 * * *", name="nightly")(cleanup)


def test_registering_the_same_handler_again_replaces_it() -> None:
    # Importing a module again, such as on reload, decorates its functions again.
    def reindex() -> None: ...

    schedule("0 3 * * *")(reindex)
    schedule("0 4 * * *")(reindex)

    assert [str(s.cron) for s in get_registered_schedules()] == ["0 4 * * *"]


@pytest.mark.parametrize(
    ("cron", "options", "match"),
    [
        ("0 3 * *", {}, "must have 5 fields"),
        ("0 3 * * *", {"timezone": "Mars/Olympus_Mons"}, "not a known IANA timezone"),
        ("0 3 * * *", {"jitter": timedelta(minutes=20)}, "jitter must be between"),
        ("0 3 * * *", {"name": "nightly report"}, "schedule name 'nightly report'"),
    ],
)
def test_rejects_invalid_options(cron: str, options: dict[str, Any], match: str) -> None:
    def reindex() -> None: ...

    with pytest.raises(SchedulesValidationError, match=match):
        schedule(cron, **options)(reindex)


def test_a_name_that_is_not_a_valid_schedule_name_needs_an_explicit_one() -> None:
    def _reindex() -> None: ...

    with pytest.raises(SchedulesValidationError, match="pass name="):
        schedule("0 3 * * *")(_reindex)


def test_rejects_a_handler_that_requires_other_arguments() -> None:
    def reindex(firing: ScheduleMetadata, batch_size: int) -> None: ...

    with pytest.raises(TypeError, match="no arguments or one ScheduleMetadata"):
        schedule("0 3 * * *")(reindex)

    assert get_registered_schedules() == ()
