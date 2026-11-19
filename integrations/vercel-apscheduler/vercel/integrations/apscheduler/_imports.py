"""Centralized APScheduler imports with one useful dependency error."""

from __future__ import annotations

from typing import Any

from importlib import import_module

try:
    from apscheduler.events import (  # type: ignore[import-untyped]
        EVENT_JOB_ERROR,
        EVENT_JOB_EXECUTED,
        EVENT_JOB_MAX_INSTANCES,
        EVENT_JOB_MISSED,
        JobExecutionEvent,
        JobSubmissionEvent,
    )
    from apscheduler.jobstores.base import (  # type: ignore[import-untyped]
        ConflictingIdError,
        JobLookupError,
    )
    from apscheduler.jobstores.memory import MemoryJobStore  # type: ignore[import-untyped]
    from apscheduler.schedulers.base import BaseScheduler  # type: ignore[import-untyped]
    from apscheduler.triggers.combining import (  # type: ignore[import-untyped]
        AndTrigger,
        OrTrigger,
    )
    from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-untyped]
    from apscheduler.triggers.cron.expressions import (  # type: ignore[import-untyped]
        AllExpression,
        LastDayOfMonthExpression,
        RangeExpression,
        WeekdayPositionExpression,
    )
    from apscheduler.triggers.date import DateTrigger  # type: ignore[import-untyped]
    from apscheduler.triggers.interval import IntervalTrigger  # type: ignore[import-untyped]
    from apscheduler.util import undefined  # type: ignore[import-untyped]
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        'vercel-apscheduler requires APScheduler 3.x. Install "APScheduler>=3.10.4,<4".'
    ) from exc

CalendarIntervalTrigger: Any
try:
    CalendarIntervalTrigger = import_module(
        "apscheduler.triggers.calendarinterval"
    ).CalendarIntervalTrigger
except ImportError:  # pragma: no cover - added in APScheduler 3.11
    CalendarIntervalTrigger = None

__all__ = [
    "EVENT_JOB_ERROR",
    "EVENT_JOB_EXECUTED",
    "EVENT_JOB_MAX_INSTANCES",
    "EVENT_JOB_MISSED",
    "AllExpression",
    "AndTrigger",
    "BaseScheduler",
    "CalendarIntervalTrigger",
    "ConflictingIdError",
    "CronTrigger",
    "DateTrigger",
    "IntervalTrigger",
    "JobExecutionEvent",
    "JobLookupError",
    "JobSubmissionEvent",
    "LastDayOfMonthExpression",
    "MemoryJobStore",
    "OrTrigger",
    "RangeExpression",
    "WeekdayPositionExpression",
    "undefined",
]
