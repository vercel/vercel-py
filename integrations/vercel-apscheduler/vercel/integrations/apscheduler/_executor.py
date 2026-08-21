from __future__ import annotations

from typing import Any

import atexit
import logging
import sys
import threading
from collections.abc import Awaitable
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from inspect import isawaitable
from traceback import format_tb

from anyio.from_thread import BlockingPortal, start_blocking_portal

from ._imports import (
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MISSED,
    BaseExecutor,
    JobExecutionEvent,
    MaxInstancesReachedError,
)
from ._time import as_utc

UTC = timezone.utc

__all__ = ["VercelInlineExecutor"]


class _JobPortal:
    """The process's dedicated event loop thread for coroutine jobs.

    Every coroutine job runs on this one loop regardless of where the
    scheduler was woken from: the queue worker delivers on its own event
    loop thread, where a job cannot be awaited inline, and a fresh loop
    per run would invalidate loop-bound resources (async clients, locks)
    between runs. A single persistent loop preserves the contract a stock
    AsyncIOScheduler gives jobs by running them all on its loop.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._portal: BlockingPortal | None = None

    def get(self) -> BlockingPortal:
        """Return the portal, starting its loop thread on first use."""
        with self._lock:
            if self._portal is None:
                stack = ExitStack()
                self._portal = stack.enter_context(start_blocking_portal())
                atexit.register(stack.close)
            return self._portal


_JOB_PORTAL = _JobPortal()


async def _await_result(value: Awaitable[object]) -> object:
    return await value


def _run_awaitable(value: Awaitable[object]) -> object:
    return _JOB_PORTAL.get().call(_await_result, value)


def _run_job_at_reference_time(
    job: Any,
    jobstore_alias: str,
    run_times: list[datetime],
    logger_name: str,
    reference_time: datetime | None,
) -> list[Any]:
    events: list[Any] = []
    logger = logging.getLogger(logger_name)
    effective_reference_time = (
        as_utc(reference_time, name="reference_time")
        if reference_time is not None
        else datetime.now(UTC)
    )

    for run_time in run_times:
        if job.misfire_grace_time is not None:
            difference = effective_reference_time - as_utc(run_time, name="run_time")
            grace_time = timedelta(seconds=job.misfire_grace_time)
            if difference > grace_time:
                events.append(
                    JobExecutionEvent(EVENT_JOB_MISSED, job.id, jobstore_alias, run_time),
                )
                logger.warning('Run time of job "%s" was missed by %s', job, difference)
                continue

        logger.info('Running job "%s" (scheduled at %s)', job, run_time)
        try:
            retval = job.func(*job.args, **job.kwargs)
            if isawaitable(retval):
                retval = _run_awaitable(retval)
        except BaseException:
            exc, tb = sys.exc_info()[1:]
            formatted_tb = "".join(format_tb(tb))
            events.append(
                JobExecutionEvent(
                    EVENT_JOB_ERROR,
                    job.id,
                    jobstore_alias,
                    run_time,
                    exception=exc,
                    traceback=formatted_tb,
                )
            )
            logger.exception('Job "%s" raised an exception', job)
        else:
            events.append(
                JobExecutionEvent(
                    EVENT_JOB_EXECUTED,
                    job.id,
                    jobstore_alias,
                    run_time,
                    retval=retval,
                )
            )
            logger.info('Job "%s" executed successfully', job)

    return events


class VercelInlineExecutor(BaseExecutor):
    """Inline executor that evaluates misfires against a wakeup logical time."""

    def __init__(self) -> None:
        super().__init__()
        self._reference_time: datetime | None = None

    def set_reference_time(self, reference_time: datetime) -> None:
        self._reference_time = reference_time

    def submit_job(self, job: Any, run_times: list[datetime]) -> None:
        assert self._lock is not None, "This executor has not been started yet"
        with self._lock:
            if self._instances[job.id] >= job.max_instances:
                raise MaxInstancesReachedError(job)
            self._instances[job.id] += 1

        self._run_inline_job(job, run_times)

    def _run_inline_job(self, job: Any, run_times: list[datetime]) -> None:
        try:
            events = _run_job_at_reference_time(
                job,
                job._jobstore_alias,
                run_times,
                self._logger.name,
                self._reference_time,
            )
        except BaseException:
            self._run_job_error(job.id, *sys.exc_info()[1:])
        else:
            self._run_job_success(job.id, events)
        finally:
            self._reference_time = None

    def _do_submit_job(self, job: Any, run_times: list[datetime]) -> None:
        self._run_inline_job(job, run_times)
