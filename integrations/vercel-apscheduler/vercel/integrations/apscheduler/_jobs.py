"""Jobs declared in code, each deployed and fired as one Vercel schedule."""

from __future__ import annotations

from typing import Any

import functools
import inspect
import random
import re
import threading
import traceback
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import anyio
from anyio import to_thread

from vercel.schedules import ScheduleMetadata, schedule

from ._errors import APSchedulerConfigurationError
from ._imports import (
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MAX_INSTANCES,
    EVENT_JOB_MISSED,
    BaseScheduler,
    ConflictingIdError,
    JobExecutionEvent,
    JobSubmissionEvent,
)
from ._triggers import (
    cover,
    run_time_in_minute,
    runs_more_than_once_a_minute,
    without_jitter,
)

__all__ = ["DeclaredJob", "declare", "declared_jobs", "fire", "in_firing"]

# The names the Schedules API accepts. A job's id names its schedule.
_SCHEDULE_NAME = re.compile(r"[0-9A-Za-z][0-9A-Za-z._-]{0,255}")
# Stock executors file job events under the job's store.
_JOBSTORE = "default"

_in_firing: ContextVar[bool] = ContextVar("vercel_apscheduler_in_firing", default=False)


@dataclass(eq=False)
class DeclaredJob:
    """A job and the schedule it deploys as.

    Attributes:
        scheduler: The scheduler the job was added to.
        job: The pending APScheduler job.
        trigger: The job's trigger without jitter, which decides whether a
            firing runs the job.
        running: Runs in progress in this process, for `max_instances`.

    """

    scheduler: BaseScheduler
    job: Any
    trigger: Any
    running: int = 0


_declared: dict[str, DeclaredJob] = {}
_declared_lock = threading.Lock()


def in_firing() -> bool:
    """Whether the caller runs inside a job that a firing started."""
    return _in_firing.get()


def declared_jobs(scheduler: BaseScheduler) -> list[DeclaredJob]:
    """Return the jobs `scheduler` declares, in the order it declared them."""
    with _declared_lock:
        return [declared for declared in _declared.values() if declared.scheduler is scheduler]


def declare(scheduler: BaseScheduler, job: Any, *, replace_existing: bool) -> None:
    """Register `job` as a schedule named after its id.

    Raises:
        APSchedulerConfigurationError: If the id can't name a schedule, the
            trigger runs more than once a minute, or another scheduler
            declares the same id.
        ConflictingIdError: If the scheduler already has a job with this id
            and `replace_existing` is false, as when the scheduler starts.

    """
    job_id = job.id
    if not _SCHEDULE_NAME.fullmatch(job_id):
        raise APSchedulerConfigurationError(
            f"job id {job_id!r} can't name a Vercel schedule: use up to 256 letters, "
            "numbers, '.', '_' and '-', starting with a letter or number"
        )
    if runs_more_than_once_a_minute(job.trigger):
        raise APSchedulerConfigurationError(
            f"job {job_id!r} runs more than once a minute, but a Vercel schedule fires "
            "at most once a minute"
        )
    with _declared_lock:
        existing = _declared.get(job_id)
        if existing is not None:
            if existing.scheduler is not scheduler:
                raise APSchedulerConfigurationError(
                    f"two schedulers declare job id {job_id!r}, but each job deploys as a "
                    "schedule named after its id; give one of them another id"
                )
            if not replace_existing:
                raise ConflictingIdError(job_id)
        declared = DeclaredJob(scheduler, job, without_jitter(job.trigger))
        covered = cover(job.trigger)
        schedule(cron=covered.cron, name=job_id, timezone=covered.timezone)(_handler(declared))
        _declared[job_id] = declared


def _handler(declared: DeclaredJob) -> Callable[[ScheduleMetadata], Awaitable[None]]:
    async def run(metadata: ScheduleMetadata) -> None:
        await fire(declared, metadata)

    # The registry treats a handler with the same name and module as the same
    # definition, so declaring an id again with `replace_existing` replaces
    # its schedule, and a `@schedule` function of the same name is a conflict.
    run.__qualname__ = f"<APScheduler job {declared.job.id!r}>"
    return run


async def fire(declared: DeclaredJob, metadata: ScheduleMetadata) -> None:
    """Run the job if its trigger runs in the minute `metadata` fired for.

    Raises:
        Exception: Whatever the job raised, after listeners hear of it.

    """
    scheduler, job = declared.scheduler, declared.job
    logger = scheduler._logger  # noqa: SLF001 - stock executors log to it
    scheduled_at = metadata.scheduled_at
    if scheduled_at.second or scheduled_at.microsecond:
        # Cron firings are scheduled on the minute; one invoked by hand, such
        # as from the dashboard, is scheduled when it was invoked. Run it now,
        # as `job.modify(next_run_time=now)` would.
        run_time = scheduled_at
    else:
        due = run_time_in_minute(declared.trigger, scheduled_at)
        if due is None:
            return
        run_time = due
        grace = _option(scheduler, job, "misfire_grace_time")
        late = datetime.now(UTC) - run_time
        if grace is not None and late > timedelta(seconds=grace):
            _dispatch(scheduler, JobExecutionEvent(EVENT_JOB_MISSED, job.id, _JOBSTORE, run_time))
            logger.warning('Run time of job "%s" was missed by %s', job, late)
            return

    max_instances = _option(scheduler, job, "max_instances")
    if declared.running >= max_instances:
        _dispatch(
            scheduler,
            JobSubmissionEvent(EVENT_JOB_MAX_INSTANCES, job.id, _JOBSTORE, [run_time]),
        )
        logger.warning(
            'Execution of job "%s" skipped: maximum number of running instances reached (%d)',
            job,
            max_instances,
        )
        return

    declared.running += 1
    try:
        jitter = getattr(job.trigger, "jitter", None)
        if jitter:
            await anyio.sleep(random.uniform(0, jitter))  # noqa: S311 - spreads load
        logger.info('Running job "%s" (scheduled at %s)', job, run_time)
        token = _in_firing.set(True)
        try:
            retval = await _call(job)
        except Exception as exc:
            _dispatch(
                scheduler,
                JobExecutionEvent(
                    EVENT_JOB_ERROR,
                    job.id,
                    _JOBSTORE,
                    run_time,
                    exception=exc,
                    traceback="".join(traceback.format_tb(exc.__traceback__)),
                ),
            )
            raise
        finally:
            _in_firing.reset(token)
        _dispatch(
            scheduler,
            JobExecutionEvent(EVENT_JOB_EXECUTED, job.id, _JOBSTORE, run_time, retval=retval),
        )
        logger.info('Job "%s" executed successfully', job)
    finally:
        declared.running -= 1


async def _call(job: Any) -> Any:
    func = functools.partial(job.func, *job.args, **job.kwargs)
    if inspect.iscoroutinefunction(job.func):
        return await func()
    result = await to_thread.run_sync(func)
    # A sync wrapper around a coroutine function returns the coroutine.
    if inspect.isawaitable(result):
        result = await result
    return result


def _option(scheduler: BaseScheduler, job: Any, name: str) -> Any:
    # A pending job has only the options it was added with; the scheduler
    # fills in the rest when it starts, which a deployment never does.
    return getattr(job, name, scheduler._job_defaults[name])  # noqa: SLF001


def _dispatch(scheduler: BaseScheduler, event: Any) -> None:
    scheduler._dispatch_event(event)  # noqa: SLF001 - how executors notify listeners
