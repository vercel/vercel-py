"""Schedule firings, as passed to a schedule entrypoint's environment."""

import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone

from vercel.schedules._internal.errors import SchedulesError

SCHEDULE_EVENT_VERSION = "1"
"""Version of the `VERCEL_SCHEDULE_*` environment contract this SDK reads."""

_VERSION_VAR = "VERCEL_SCHEDULE_EVENT_VERSION"


class ScheduleEventError(SchedulesError):
    """Raised when the environment does not hold a valid schedule firing."""


class NotAScheduleInvocationError(ScheduleEventError):
    """Raised when the process was not started by a schedule firing.

    For example, when a schedule entrypoint is run by hand.
    """


def _require(env: Mapping[str, str], name: str) -> str:
    value = env.get(name)
    if not value:
        raise ScheduleEventError(f"{name} is not set")
    return value


@dataclass(frozen=True)
class ScheduleEvent:
    """One firing of a schedule.

    Attributes:
        schedule_id: Identifies the schedule.
        execution_id: Identifies this firing. Retries of a firing share it.
        name: Schedule name, unique within its project and namespace.
        namespace: Namespace the schedule belongs to.
        scheduled_at: When the firing was scheduled to run, in UTC.
        source: How the schedule was defined, such as `"static"`.
    """

    schedule_id: str
    execution_id: str
    name: str
    namespace: str
    scheduled_at: datetime
    source: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "ScheduleEvent":
        """Read the current firing from `VERCEL_SCHEDULE_*` variables.

        The platform sets them each time it runs a schedule entrypoint.

        Args:
            env: Variables to read instead of `os.environ`.

        Raises:
            NotAScheduleInvocationError: If `VERCEL_SCHEDULE_EVENT_VERSION` is
                not set.
            ScheduleEventError: If the contract version is unsupported or a
                variable is missing or invalid.
        """
        env = os.environ if env is None else env
        version = env.get(_VERSION_VAR)
        if version is None:
            raise NotAScheduleInvocationError(f"{_VERSION_VAR} is not set")
        if version != SCHEDULE_EVENT_VERSION:
            raise ScheduleEventError(
                f"unsupported schedule event version {version!r}: "
                f"expected {SCHEDULE_EVENT_VERSION!r}"
            )

        raw_scheduled_at = _require(env, "VERCEL_SCHEDULE_SCHEDULED_AT")
        try:
            scheduled_at = datetime.fromisoformat(raw_scheduled_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ScheduleEventError(
                f"invalid VERCEL_SCHEDULE_SCHEDULED_AT {raw_scheduled_at!r}"
            ) from exc
        if scheduled_at.utcoffset() is None:
            raise ScheduleEventError(
                f"invalid VERCEL_SCHEDULE_SCHEDULED_AT {raw_scheduled_at!r}: missing offset"
            )

        return cls(
            schedule_id=_require(env, "VERCEL_SCHEDULE_ID"),
            execution_id=_require(env, "VERCEL_SCHEDULE_EXECUTION_ID"),
            name=_require(env, "VERCEL_SCHEDULE_NAME"),
            namespace=_require(env, "VERCEL_SCHEDULE_NAMESPACE"),
            scheduled_at=scheduled_at.astimezone(timezone.utc),
            source=_require(env, "VERCEL_SCHEDULE_SOURCE"),
        )


def get_event() -> ScheduleEvent:
    """Read the current firing; shorthand for `ScheduleEvent.from_env()`."""
    return ScheduleEvent.from_env()
