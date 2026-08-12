"""Centralized APScheduler imports with one useful dependency error."""

from __future__ import annotations

try:
    from apscheduler.events import (  # type: ignore[import-untyped]
        EVENT_JOB_ERROR,
        EVENT_JOB_EXECUTED,
        EVENT_JOB_MAX_INSTANCES,
        EVENT_JOB_MISSED,
        EVENT_JOB_REMOVED,
        EVENT_JOB_SUBMITTED,
        JobEvent,
        JobExecutionEvent,
        JobSubmissionEvent,
    )
    from apscheduler.executors.base import (  # type: ignore[import-untyped]
        BaseExecutor,
        MaxInstancesReachedError,
    )
    from apscheduler.jobstores.redis import (  # type: ignore[import-untyped]
        RedisJobStore,
    )
    from apscheduler.schedulers.base import (  # type: ignore[import-untyped]
        STATE_PAUSED,
        STATE_RUNNING,
        STATE_STOPPED,
        BaseScheduler,
    )
    from apscheduler.triggers.interval import (  # type: ignore[import-untyped]
        IntervalTrigger,
    )
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "vercel-apscheduler requires APScheduler 3.x and redis. "
        'Install "APScheduler>=3.10.4,<4" and "redis>=5,<7".'
    ) from exc

__all__ = [
    "EVENT_JOB_ERROR",
    "EVENT_JOB_EXECUTED",
    "EVENT_JOB_MAX_INSTANCES",
    "EVENT_JOB_MISSED",
    "EVENT_JOB_REMOVED",
    "EVENT_JOB_SUBMITTED",
    "STATE_PAUSED",
    "STATE_RUNNING",
    "STATE_STOPPED",
    "BaseExecutor",
    "BaseScheduler",
    "IntervalTrigger",
    "JobEvent",
    "JobExecutionEvent",
    "JobSubmissionEvent",
    "MaxInstancesReachedError",
    "RedisJobStore",
]
