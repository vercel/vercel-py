"""Schedule firings, as passed to a schedule command's environment."""

import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime

from vercel.schedules._internal.errors import NotAScheduleInvocationError, ScheduleEventParseError

# Set once per firing, unlike client settings such as `VERCEL_SCHEDULE_BASE_URL`
# that a process can inherit without being a firing.
_EXECUTION_ID_VAR = "VERCEL_SCHEDULE_EXECUTION_ID"
_SCHEDULED_AT_VAR = "VERCEL_SCHEDULE_SCHEDULED_AT"


def _require(env: Mapping[str, str], name: str) -> str:
    value = env.get(name)
    if not value:
        raise ScheduleEventParseError(f"{name} is not set")
    return value


def _scheduled_at(env: Mapping[str, str]) -> datetime:
    raw = _require(env, _SCHEDULED_AT_VAR)
    try:
        value = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ScheduleEventParseError(f"invalid {_SCHEDULED_AT_VAR} {raw!r}") from exc
    if value.utcoffset() is None:
        raise ScheduleEventParseError(f"invalid {_SCHEDULED_AT_VAR} {raw!r}: missing offset")
    return value.astimezone(UTC)


@dataclass(frozen=True)
class ScheduleMetadata:
    """One firing of a schedule.

    Attributes:
        schedule_id: Identifies the schedule.
        execution_id: Identifies this firing.
        name: Schedule name, unique within its project and namespace.
        namespace: Namespace the schedule belongs to.
        scheduled_at: When the firing was scheduled to run, in UTC. It does not
            include jitter, so the firing may start later.
        source: How the schedule was defined, such as `"static"`.
        expression: The schedule's cron expression, or the local datetime of a
            one-time schedule.
    """

    schedule_id: str
    execution_id: str
    name: str
    namespace: str
    scheduled_at: datetime
    source: str
    expression: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "ScheduleMetadata":
        """Read the current firing from `VERCEL_SCHEDULE_*` variables.

        The platform sets them each time it runs a schedule command.

        Args:
            env: Variables to read instead of `os.environ`.

        Raises:
            NotAScheduleInvocationError: If `VERCEL_SCHEDULE_EXECUTION_ID` is not
                set, such as when the command is run by hand.
            ScheduleEventParseError: If another variable is missing or invalid.
        """
        env = os.environ if env is None else env
        if not env.get(_EXECUTION_ID_VAR):
            raise NotAScheduleInvocationError(f"{_EXECUTION_ID_VAR} is not set")

        return cls(
            schedule_id=_require(env, "VERCEL_SCHEDULE_ID"),
            execution_id=env[_EXECUTION_ID_VAR],
            name=_require(env, "VERCEL_SCHEDULE_NAME"),
            namespace=_require(env, "VERCEL_SCHEDULE_NAMESPACE"),
            scheduled_at=_scheduled_at(env),
            source=_require(env, "VERCEL_SCHEDULE_SOURCE"),
            expression=_require(env, "VERCEL_SCHEDULE_EXPRESSION"),
        )


__all__ = ["ScheduleMetadata"]
