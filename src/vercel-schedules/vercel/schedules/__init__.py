"""Vercel Schedules SDK.

A schedule command runs once per firing. Read the firing with
`ScheduleMetadata.from_env()`.
"""

from vercel.schedules._internal.errors import (
    NotAScheduleInvocationError,
    ScheduleEventParseError,
    SchedulesError,
)
from vercel.schedules._internal.metadata import ScheduleMetadata
from vercel.schedules.version import __version__

# Only add public symbols to __all__; internal helpers must stay unexported.
__all__ = [
    "NotAScheduleInvocationError",
    "ScheduleEventParseError",
    "ScheduleMetadata",
    "SchedulesError",
    "__version__",
]
