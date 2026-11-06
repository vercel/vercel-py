"""Schedules errors for the SDK surface."""

from vercel._internal.core.errors import VercelError


class SchedulesError(VercelError):
    """Base error for Vercel Schedules operations."""


class ScheduleEventError(SchedulesError):
    """Raised when the environment does not hold a valid schedule firing."""


class NotAScheduleInvocationError(ScheduleEventError):
    """Raised when the process was not started by a schedule firing.

    For example, when a schedule command is run by hand.
    """


__all__ = [
    "NotAScheduleInvocationError",
    "ScheduleEventError",
    "SchedulesError",
]
