from __future__ import annotations

from typing import Any

import threading
from datetime import UTC, datetime

import anyio
import anyio.lowlevel
import pytest
import time_machine
from apscheduler.events import (
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MAX_INSTANCES,
    EVENT_JOB_MISSED,
)
from apscheduler.jobstores.base import BaseJobStore, ConflictingIdError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.calendarinterval import CalendarIntervalTrigger
from apscheduler.triggers.date import DateTrigger

from vercel.integrations.apscheduler import APSchedulerConfigurationError, _jobs
from vercel.schedules import (
    RegisteredSchedule,
    SchedulesValidationError,
    get_registered_schedules,
    schedule as vercel_schedule,
)

from .conftest import firing

pytestmark = pytest.mark.usefixtures("installed")

MONTH_END = datetime(2026, 11, 30, 23, tzinfo=UTC)


def registered(name: str) -> RegisteredSchedule:
    return {schedule.name: schedule for schedule in get_registered_schedules()}[name]


def deployed() -> list[tuple[str, str, str | None]]:
    return [
        (schedule.name, str(schedule.cron), schedule.timezone)
        for schedule in get_registered_schedules()
    ]


def test_each_job_deploys_as_a_schedule_named_after_its_id() -> None:
    scheduler = BlockingScheduler(timezone="America/New_York")

    @scheduler.scheduled_job("cron", day_of_week="mon-fri", hour=9, minute=30, id="report")
    def report() -> None: ...

    scheduler.add_job(report, "interval", minutes=5, id="poll")

    assert deployed() == [
        ("report", "30 9 * * 1-5", "America/New_York"),
        # Without a start date, an interval counts from the Unix epoch.
        ("poll", "*/5 * * * *", "UTC"),
    ]


async def test_a_firing_runs_the_job_only_in_a_minute_its_trigger_runs_in() -> None:
    runs: list[str] = []
    scheduler = BackgroundScheduler(timezone=UTC)
    scheduler.add_job(runs.append, "cron", day="last", hour=23, args=["ran"], id="month-end")

    await registered("month-end").run(firing("month-end", MONTH_END))
    await registered("month-end").run(firing("month-end", datetime(2026, 11, 29, 23, tzinfo=UTC)))

    assert runs == ["ran"]


async def test_a_firing_invoked_by_hand_runs_the_job_now() -> None:
    runs: list[str] = []
    scheduler = BackgroundScheduler(timezone=UTC)
    scheduler.add_job(runs.append, "cron", day="last", hour=23, args=["ran"], id="month-end")

    invoked_at = datetime(2026, 11, 29, 10, 4, 7, 123000, tzinfo=UTC)
    await registered("month-end").run(firing("month-end", invoked_at))

    assert runs == ["ran"]


async def test_runs_a_sync_job_in_a_worker_thread_and_awaits_a_coroutine_job() -> None:
    threads: list[threading.Thread] = []
    awaited: list[bool] = []
    scheduler = AsyncIOScheduler(timezone=UTC)

    @scheduler.scheduled_job("cron", hour=23, id="sync-job")
    def sync_job() -> None:
        threads.append(threading.current_thread())

    @scheduler.scheduled_job("cron", hour=23, id="async-job")
    async def async_job() -> None:
        await anyio.lowlevel.checkpoint()
        awaited.append(True)

    await registered("sync-job").run(firing("sync-job", MONTH_END))
    await registered("async-job").run(firing("async-job", MONTH_END))

    assert threads
    assert threads[0] is not threading.current_thread()
    assert awaited == [True]


async def test_listeners_hear_of_each_run_and_errors_propagate() -> None:
    events: list[Any] = []
    scheduler = BackgroundScheduler(timezone=UTC)
    scheduler.add_listener(events.append, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    @scheduler.scheduled_job("cron", hour=23, id="works")
    def works() -> int:
        return 7

    @scheduler.scheduled_job("cron", hour=23, id="fails")
    def fails() -> None:
        raise ValueError("boom")

    await registered("works").run(firing("works", MONTH_END))
    with pytest.raises(ValueError, match="boom"):
        await registered("fails").run(firing("fails", MONTH_END))

    assert [(event.code, event.job_id, event.scheduled_run_time) for event in events] == [
        (EVENT_JOB_EXECUTED, "works", MONTH_END),
        (EVENT_JOB_ERROR, "fails", MONTH_END),
    ]
    assert events[0].retval == 7
    assert isinstance(events[1].exception, ValueError)
    assert "fails" in events[1].traceback


async def test_runs_a_late_firing_unless_the_job_sets_a_misfire_grace_time() -> None:
    runs: list[str] = []
    missed: list[Any] = []
    scheduler = BackgroundScheduler(timezone=UTC)
    scheduler.add_listener(missed.append, EVENT_JOB_MISSED)
    scheduler.add_job(runs.append, "cron", hour=23, args=["any"], id="any-time")
    scheduler.add_job(
        runs.append, "cron", hour=23, args=["strict"], id="strict", misfire_grace_time=30
    )

    with time_machine.travel(datetime(2026, 11, 30, 23, 1, tzinfo=UTC), tick=False):
        await registered("any-time").run(firing("any-time", MONTH_END))
        await registered("strict").run(firing("strict", MONTH_END))

    assert runs == ["any"]
    assert [(event.job_id, event.scheduled_run_time) for event in missed] == [("strict", MONTH_END)]


def test_keeps_an_explicit_default_misfire_grace_time() -> None:
    scheduler = BackgroundScheduler(job_defaults={"misfire_grace_time": 30})
    assert scheduler._job_defaults["misfire_grace_time"] == 30
    assert BackgroundScheduler()._job_defaults["misfire_grace_time"] is None


async def test_skips_a_firing_while_max_instances_runs_are_in_progress() -> None:
    started = anyio.Event()
    release = anyio.Event()
    skipped: list[Any] = []
    scheduler = AsyncIOScheduler(timezone=UTC)
    scheduler.add_listener(skipped.append, EVENT_JOB_MAX_INSTANCES)

    @scheduler.scheduled_job("cron", hour=23, id="slow")
    async def slow() -> None:
        started.set()
        await release.wait()

    async with anyio.create_task_group() as group:
        group.start_soon(registered("slow").run, firing("slow", MONTH_END))
        await started.wait()
        await registered("slow").run(firing("slow", MONTH_END))
        release.set()

    assert [event.job_id for event in skipped] == ["slow"]


async def test_sleeps_up_to_the_trigger_jitter_before_running(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr(_jobs.random, "uniform", lambda low, high: high)
    monkeypatch.setattr(_jobs.anyio, "sleep", sleep)
    scheduler = BackgroundScheduler(timezone=UTC)
    scheduler.add_job(lambda: None, "cron", hour=23, jitter=20, id="spread")

    await registered("spread").run(firing("spread", MONTH_END))

    assert slept == [20]


def test_start_and_shutdown_leave_the_scheduler_to_the_deployment() -> None:
    scheduler = BlockingScheduler()
    scheduler.add_job(print, "cron", hour=3, id="nightly")

    scheduler.start()
    scheduler.shutdown()

    assert not scheduler.running
    assert [job.id for job in scheduler.get_jobs()] == ["nightly"]


def test_rejects_jobs_added_after_start() -> None:
    scheduler = BackgroundScheduler()
    scheduler.start()

    with pytest.raises(APSchedulerConfigurationError, match="at runtime"):
        scheduler.add_job(print, "cron", hour=3, id="late")
    assert deployed() == []


async def test_rejects_jobs_added_by_a_running_job() -> None:
    scheduler = BackgroundScheduler(timezone=UTC)

    @scheduler.scheduled_job("cron", hour=23, id="spawner")
    def spawner() -> None:
        scheduler.add_job(print, "cron", hour=4, id="spawned")

    with pytest.raises(APSchedulerConfigurationError, match="at runtime"):
        await registered("spawner").run(firing("spawner", MONTH_END))


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({}, "needs an id"),
        ({"id": "no spaces"}, "can't name a Vercel schedule"),
        ({"id": "soon", "next_run_time": datetime(2026, 1, 1, tzinfo=UTC)}, "next_run_time"),
        ({"id": "fast", "trigger": "interval", "seconds": 30}, "more than once a minute"),
        ({"id": "fast", "trigger": "cron", "second": "*/30"}, "more than once a minute"),
    ],
)
def test_rejects_jobs_a_schedule_cannot_run(kwargs: dict[str, Any], message: str) -> None:
    scheduler = BackgroundScheduler()
    kwargs.setdefault("trigger", "cron")

    with pytest.raises(APSchedulerConfigurationError, match=message):
        scheduler.add_job(print, **kwargs)
    assert scheduler.get_jobs() == []
    assert deployed() == []


def test_rejects_triggers_that_depend_on_when_the_process_started() -> None:
    with pytest.raises(APSchedulerConfigurationError, match="needs run_date"):
        BackgroundScheduler().add_job(print, id="once")
    with pytest.raises(APSchedulerConfigurationError, match="needs run_date"):
        DateTrigger()
    with pytest.raises(APSchedulerConfigurationError, match="needs start_date"):
        CalendarIntervalTrigger(weeks=2)
    assert CalendarIntervalTrigger(days=1, hour=3).start_date


def test_declaring_an_id_again_replaces_its_schedule_only_when_asked() -> None:
    scheduler = BackgroundScheduler(timezone=UTC)

    @scheduler.scheduled_job("cron", hour=3, id="nightly")
    @scheduler.scheduled_job("cron", hour=4, id="nightly")
    def nightly() -> None: ...

    assert deployed() == [("nightly", "0 3 * * *", "UTC")]
    with pytest.raises(ConflictingIdError):
        scheduler.add_job(nightly, "cron", hour=5, id="nightly")
    assert deployed() == [("nightly", "0 3 * * *", "UTC")]


def test_rejects_an_id_two_schedulers_declare() -> None:
    BackgroundScheduler().add_job(print, "cron", hour=3, id="nightly")

    with pytest.raises(APSchedulerConfigurationError, match="two schedulers"):
        BackgroundScheduler().add_job(print, "cron", hour=4, id="nightly")


def test_a_schedule_function_of_the_same_name_conflicts() -> None:
    BackgroundScheduler().add_job(print, "cron", hour=3, id="nightly")

    with pytest.raises(SchedulesValidationError, match="already declared"):

        @vercel_schedule(cron="0 4 * * *", name="nightly")
        def nightly() -> None: ...


def test_rejects_job_stores_that_hold_jobs_outside_code() -> None:
    class DatabaseJobStore(BaseJobStore):  # type: ignore[misc]
        def lookup_job(self, job_id: str) -> None: ...
        def get_due_jobs(self, now: datetime) -> list[Any]:
            return []

        def get_next_run_time(self) -> None: ...
        def get_all_jobs(self) -> list[Any]:
            return []

        def add_job(self, job: Any) -> None: ...
        def update_job(self, job: Any) -> None: ...
        def remove_job(self, job_id: str) -> None: ...
        def remove_all_jobs(self) -> None: ...

    with pytest.raises(APSchedulerConfigurationError, match='job store "default"'):
        BackgroundScheduler(jobstores={"default": DatabaseJobStore()})
    scheduler = BackgroundScheduler()
    scheduler.add_jobstore("memory", "other")
    with pytest.raises(APSchedulerConfigurationError, match='job store "db"'):
        scheduler.add_jobstore(DatabaseJobStore(), "db")
    assert set(scheduler._jobstores) == {"other"}


@pytest.mark.parametrize(
    ("method", "args", "kwargs"),
    [
        ("modify_job", ("nightly",), {"max_instances": 2}),
        ("reschedule_job", ("nightly",), {"trigger": "cron", "hour": 4}),
        ("remove_job", ("nightly",), {}),
        ("remove_all_jobs", (), {}),
    ],
)
def test_jobs_cannot_change_once_declared(
    method: str, args: tuple[Any, ...], kwargs: dict[str, Any]
) -> None:
    scheduler = BackgroundScheduler(timezone=UTC)
    scheduler.add_job(print, "cron", hour=3, id="nightly")

    with pytest.raises(APSchedulerConfigurationError, match="change its declaration"):
        getattr(scheduler, method)(*args, **kwargs)
    assert deployed() == [("nightly", "0 3 * * *", "UTC")]
