"""Vercel Schedules SDK.

A schedule command runs once per firing. Read the firing with
`ScheduleEvent.from_env()`.
"""

from vercel.schedules._internal.errors import (
    NotAScheduleInvocationError,
    ScheduleEventError,
    SchedulesError,
)
from vercel.schedules._internal.event import ScheduleEvent
from vercel.schedules.version import __version__

# Only add public symbols to __all__; internal helpers must stay unexported.
__all__ = [
    "NotAScheduleInvocationError",
    "ScheduleEvent",
    "ScheduleEventError",
    "SchedulesError",
    "__version__",
]
