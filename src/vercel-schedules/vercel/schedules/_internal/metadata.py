"""Schedule firings, read from a schedule command's environment or a dispatch request."""

import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime

from vercel.schedules._internal.errors import NotAScheduleInvocationError, ScheduleEventParseError

SCHEDULE_CLOUD_EVENT_TYPE = "com.vercel.schedule.v1beta"
"""The CloudEvent type of a schedule dispatch, which is also its protocol version."""

# Set once per firing, unlike client settings such as `VERCEL_SCHEDULE_BASE_URL`
# that a process can inherit without being a firing.
_EXECUTION_ID_VAR = "VERCEL_SCHEDULE_EXECUTION_ID"
_SCHEDULED_AT_VAR = "VERCEL_SCHEDULE_SCHEDULED_AT"

_TYPE_HEADER = "ce-type"
_SPEC_VERSION_HEADER = "ce-specversion"
_SCHEDULED_AT_HEADER = "ce-vssscheduledat"


def _require(values: Mapping[str, str], name: str, label: str) -> str:
    value = values.get(name)
    if not value:
        raise ScheduleEventParseError(f"{label} is not set")
    return value


def _scheduled_at(raw: str, label: str) -> datetime:
    try:
        value = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ScheduleEventParseError(f"invalid {label} {raw!r}") from exc
    if value.utcoffset() is None:
        raise ScheduleEventParseError(f"invalid {label} {raw!r}: missing offset")
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

        def require(name: str) -> str:
            return _require(env, name, name)

        return cls(
            schedule_id=require("VERCEL_SCHEDULE_ID"),
            execution_id=env[_EXECUTION_ID_VAR],
            name=require("VERCEL_SCHEDULE_NAME"),
            namespace=require("VERCEL_SCHEDULE_NAMESPACE"),
            scheduled_at=_scheduled_at(require(_SCHEDULED_AT_VAR), _SCHEDULED_AT_VAR),
            source=require("VERCEL_SCHEDULE_SOURCE"),
            expression=require("VERCEL_SCHEDULE_EXPRESSION"),
        )

    @classmethod
    def from_headers(cls, headers: Mapping[str, str]) -> "ScheduleMetadata":
        """Read a firing from the headers of a schedule dispatch request.

        A dispatch is a binary-mode CloudEvent with no body. Header names are
        case-insensitive, and the request method is not checked.

        Args:
            headers: The request headers.

        Raises:
            NotAScheduleInvocationError: If the request has no `ce-type`
                header, such as a direct request or a legacy Cron Job.
            ScheduleEventParseError: If the CloudEvent is not a supported schedule
                dispatch, or a required header is missing or invalid.
        """
        values = {name.lower(): value for name, value in headers.items()}
        event_type = values.get(_TYPE_HEADER)
        if not event_type:
            raise NotAScheduleInvocationError(f"{_TYPE_HEADER} header is not set")
        spec_version = values.get(_SPEC_VERSION_HEADER)
        if spec_version != "1.0":
            raise ScheduleEventParseError(f"unsupported CloudEvent spec version {spec_version!r}")
        if event_type != SCHEDULE_CLOUD_EVENT_TYPE:
            raise ScheduleEventParseError(f"unsupported CloudEvent type {event_type!r}")

        def require(name: str) -> str:
            return _require(values, name, f"{name} header")

        require("ce-source")
        return cls(
            schedule_id=require("ce-vssscheduleid"),
            execution_id=require("ce-id"),
            name=require("ce-vssschedulename"),
            namespace=require("ce-vssnamespace"),
            scheduled_at=_scheduled_at(require(_SCHEDULED_AT_HEADER), _SCHEDULED_AT_HEADER),
            source=require("ce-vssschedulesource"),
            expression=require("ce-vssexpression"),
        )


__all__ = ["SCHEDULE_CLOUD_EVENT_TYPE", "ScheduleMetadata"]
