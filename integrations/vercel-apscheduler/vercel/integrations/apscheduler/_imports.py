from __future__ import annotations

try:
    from apscheduler.events import (  # type: ignore[import-untyped]
        EVENT_JOB_ERROR,
        EVENT_JOB_EXECUTED,
        EVENT_JOB_MAX_INSTANCES,
        EVENT_JOB_MISSED,
        EVENT_JOB_SUBMITTED,
        JobExecutionEvent,
        JobSubmissionEvent,
    )
    from apscheduler.executors.base import (  # type: ignore[import-untyped]
        BaseExecutor,
        MaxInstancesReachedError,
    )
    from apscheduler.jobstores.base import JobLookupError  # type: ignore[import-untyped]
    from apscheduler.jobstores.memory import MemoryJobStore  # type: ignore[import-untyped]
    from apscheduler.schedulers.base import (  # type: ignore[import-untyped]
        STATE_PAUSED,
        STATE_RUNNING,
        STATE_STOPPED,
        BaseScheduler,
    )
    from apscheduler.triggers.date import DateTrigger  # type: ignore[import-untyped]
    from apscheduler.triggers.interval import IntervalTrigger  # type: ignore[import-untyped]
except ImportError as exc:  # pragma: no cover - exercised by import guards
    raise RuntimeError(
        "APScheduler is required to use vercel.integrations.apscheduler. "
        "Install it with `pip install 'APScheduler>=3.10.4,<4'`."
    ) from exc


__all__ = [
    "EVENT_JOB_ERROR",
    "EVENT_JOB_EXECUTED",
    "EVENT_JOB_MAX_INSTANCES",
    "EVENT_JOB_MISSED",
    "EVENT_JOB_SUBMITTED",
    "STATE_PAUSED",
    "STATE_RUNNING",
    "STATE_STOPPED",
    "BaseExecutor",
    "BaseScheduler",
    "DateTrigger",
    "IntervalTrigger",
    "JobExecutionEvent",
    "JobLookupError",
    "JobSubmissionEvent",
    "MaxInstancesReachedError",
    "MemoryJobStore",
]
