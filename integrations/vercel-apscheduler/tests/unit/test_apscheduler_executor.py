"""Coroutine job execution through the inline executor's portal loop."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import anyio
from anyio.lowlevel import current_token

from vercel.integrations.apscheduler._executor import (
    _run_awaitable,
    _run_job_at_reference_time,
)
from vercel.integrations.apscheduler._imports import (
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
)

UTC = timezone.utc


async def _loop_token() -> object:
    return current_token()


def test_runs_coroutine_without_a_running_loop() -> None:
    assert _run_awaitable(_loop_token()) is not None


def test_runs_coroutine_from_a_running_event_loop() -> None:
    """The queue worker delivers on its own event loop thread; a coroutine
    job must still execute instead of being rejected."""

    async def deliver() -> object:
        # Simulates a sync wakeup handler running on the worker's loop.
        return _run_awaitable(_loop_token())

    assert anyio.run(deliver) is not None


def test_coroutine_jobs_share_one_persistent_loop() -> None:
    """Loop-bound resources (async clients, locks) created by one run must
    stay valid for the next, matching a stock AsyncIOScheduler."""
    first = _run_awaitable(_loop_token())

    async def deliver() -> object:
        return _run_awaitable(_loop_token())

    second = anyio.run(deliver)
    assert first == second


def test_job_loop_is_not_the_callers_loop() -> None:
    async def deliver() -> tuple[object, object]:
        return current_token(), _run_awaitable(_loop_token())

    caller_token, job_token = anyio.run(deliver)
    assert caller_token != job_token


def _make_job(func: object) -> SimpleNamespace:
    return SimpleNamespace(
        func=func,
        args=(),
        kwargs={},
        id="job-1",
        misfire_grace_time=None,
        _jobstore_alias="default",
    )


def test_async_job_success_event_carries_the_return_value() -> None:
    async def job() -> str:
        return "ran"

    now = datetime.now(UTC)
    events = _run_job_at_reference_time(_make_job(job), "default", [now], "test", now)
    assert [event.code for event in events] == [EVENT_JOB_EXECUTED]
    assert events[0].retval == "ran"


def test_async_job_failure_becomes_a_job_error_event() -> None:
    async def job() -> None:
        msg = "boom"
        raise RuntimeError(msg)

    now = datetime.now(UTC)
    events = _run_job_at_reference_time(_make_job(job), "default", [now], "test", now)
    assert [event.code for event in events] == [EVENT_JOB_ERROR]
    assert isinstance(events[0].exception, RuntimeError)
