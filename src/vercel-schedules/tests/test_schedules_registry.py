import functools
import threading
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from vercel.schedules import (
    Cron,
    RegisteredSchedule,
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
        scheduled_at=datetime(2026, 9, 2, 3, tzinfo=UTC),
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


def test_records_the_function_settings() -> None:
    @schedule("0 4 * * *", max_duration=timedelta(minutes=15), regions=["fra1", "cdg1"])
    def export_reports() -> None: ...

    [registered] = get_registered_schedules()
    assert (registered.max_duration, registered.regions) == (
        timedelta(minutes=15),
        ("fra1", "cdg1"),
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


async def test_awaits_a_coroutine_from_a_sync_wrapper() -> None:
    received: list[ScheduleMetadata] = []

    async def reindex(firing: ScheduleMetadata) -> None:
        received.append(firing)

    @functools.wraps(reindex)
    def traced(firing: ScheduleMetadata) -> Any:
        return reindex(firing)

    schedule("0 3 * * *")(traced)
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


def test_rejects_a_second_handler_with_the_same_name() -> None:
    @schedule("0 3 * * *", name="nightly")
    def reindex() -> None: ...

    def cleanup() -> None: ...

    with pytest.raises(SchedulesValidationError, match="already declared by"):
        schedule("0 4 * * *", name="nightly")(cleanup)


def define_cleanup(module: str, source_file: str) -> Any:
    """Execute a `cleanup` definition as if it were in `module`, from `source_file`."""
    namespace: dict[str, Any] = {"__name__": module}
    exec(compile("def cleanup() -> None: ...", source_file, "exec"), namespace)
    return namespace["cleanup"]


@pytest.mark.parametrize(
    ("first", "second"),
    [
        pytest.param(
            ("jobs.tasks", "jobs/tasks.py"), ("jobs.tasks", "jobs/tasks.py"), id="module reload"
        ),
        pytest.param(
            ("jobs.tasks", "jobs/tasks.py"), ("__main__", "jobs/tasks.py"), id="run as __main__"
        ),
        pytest.param(("__main__", "cell-1"), ("__main__", "cell-2"), id="edited notebook cell"),
    ],
)
def test_a_rerun_definition_replaces_its_schedule(
    first: tuple[str, str], second: tuple[str, str]
) -> None:
    schedule("0 3 * * *")(define_cleanup(*first))
    cleanup = define_cleanup(*second)
    schedule("0 4 * * *")(cleanup)

    [registered] = get_registered_schedules()
    assert (registered.func, str(registered.cron)) == (cleanup, "0 4 * * *")


def test_rejects_a_function_of_the_same_name_in_another_module() -> None:
    schedule("0 3 * * *")(define_cleanup("jobs.tasks", "jobs/tasks.py"))
    with pytest.raises(SchedulesValidationError, match="already declared by"):
        schedule("0 4 * * *")(define_cleanup("jobs.maintenance", "jobs/maintenance.py"))


def test_rejects_one_name_stacked_twice_on_a_function() -> None:
    def report() -> None: ...

    schedule("0 15 * * *", name="report")(report)
    with pytest.raises(SchedulesValidationError, match="already declared by"):
        schedule("0 3 * * *", name="report")(report)


def test_stacked_schedules_run_one_handler() -> None:
    @schedule("0 3 * * *", name="report-morning")
    @schedule("0 15 * * *", name="report-afternoon", regions=["fra1"])
    def report() -> None: ...

    assert [(s.name, s.func, s.regions) for s in get_registered_schedules()] == [
        ("report-afternoon", report, ("fra1",)),
        ("report-morning", report, None),
    ]


@pytest.mark.parametrize(
    ("inner", "outer"),
    [(None, None), ("report-afternoon", None), (None, "report-morning")],
)
def test_stacked_schedules_each_need_a_name(inner: str | None, outer: str | None) -> None:
    def report() -> None: ...

    schedule("0 15 * * *", name=inner)(report)
    with pytest.raises(SchedulesValidationError, match="pass name= to each"):
        schedule("0 3 * * *", name=outer)(report)


@pytest.mark.parametrize(
    ("cron", "options", "match"),
    [
        ("0 3 * *", {}, "must have 5 fields"),
        ("0 3 * * *", {"timezone": "Mars/Olympus_Mons"}, "not a known IANA timezone"),
        ("0 3 * * *", {"jitter": timedelta(minutes=20)}, "jitter must be between"),
        ("0 3 * * *", {"name": "nightly report"}, "schedule name 'nightly report'"),
        ("0 3 * * *", {"max_duration": 300}, r"such as timedelta\(seconds=300\)"),
        ("0 3 * * *", {"max_duration": timedelta(0)}, "max_duration must be positive"),
        ("0 3 * * *", {"max_duration": timedelta(seconds=1.5)}, "whole number of seconds"),
        ("0 3 * * *", {"regions": "fra1"}, r"such as \['fra1'\]"),
        ("0 3 * * *", {"regions": []}, "at least one region"),
        ("0 3 * * *", {"regions": ["fra1", ""]}, "region IDs such as 'iad1'"),
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


def test_rejects_a_positional_parameter_with_a_default() -> None:
    # `run` would pass the metadata as `full`.
    def reindex(full: bool = False) -> None: ...

    with pytest.raises(TypeError, match="'full', which has a default"):
        schedule("0 3 * * *")(reindex)


async def test_a_keyword_only_option_keeps_its_default() -> None:
    received: list[bool] = []

    @schedule("0 3 * * *")
    async def reindex(*, full: bool = False) -> None:
        received.append(full)

    [registered] = get_registered_schedules()
    await registered.run(metadata("reindex"))

    assert received == [False]
