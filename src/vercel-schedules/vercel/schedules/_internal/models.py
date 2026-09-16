"""Public value types for the Schedules SDK."""

from datetime import datetime, timedelta
from typing import Any, Generic, TypeAlias, TypeVar

from vercel._internal.core.polyfills import StrEnum
from vercel.schedules._internal.base import SchedulesModel

PayloadT = TypeVar("PayloadT")


class ScheduleState(StrEnum):
    """Whether a schedule currently fires."""

    ACTIVE = "active"
    INACTIVE = "inactive"


class ScheduleSource(StrEnum):
    """How a schedule was defined."""

    STATIC = "static"
    """Declared in project configuration and synced on deploy."""

    DYNAMIC = "dynamic"
    """Created at runtime through this API."""


class CronExpression(SchedulesModel):
    """A recurring schedule, in standard five-field cron syntax.

    The expression is evaluated in the schedule's `timezone`.
    """

    cron: str


class OneOffExpression(SchedulesModel):
    """A schedule that fires once, at a fixed instant.

    Attributes:
        at: Wall-clock time of the firing, as stored by the service. When the
            service reports it without an offset, it is naive and reads in the
            schedule's `timezone`.
    """

    at: datetime


ScheduleExpression: TypeAlias = CronExpression | OneOffExpression
"""When a schedule fires: on a cron cadence or once at a fixed instant."""


class QueueTarget(SchedulesModel):
    """Where a schedule dispatches: a Vercel Queue topic."""

    topic: str


class FunctionTarget(SchedulesModel):
    """Where a schedule dispatches: a function build output."""

    function: str


ScheduleTarget: TypeAlias = QueueTarget | FunctionTarget
"""Where each schedule firing is dispatched."""


class Schedule(SchedulesModel):
    """A schedule as stored by the Schedules service.

    A schedule is identified by its `name` within a `namespace`; `schedule_id`
    is the service's internal identifier and is informational.

    Attributes:
        schedule_id: Unique identifier assigned by the service.
        owner_id: Team that owns the schedule.
        project_id: Project the schedule belongs to.
        track_id: Deployment track the schedule dispatches into.
        name: Schedule name, unique within its project and namespace.
        namespace: Namespace the schedule belongs to.
        expression: When the schedule fires.
        timezone: IANA timezone the expression is evaluated in.
        jitter: Random delay added to each firing, if configured.
        target: Where each firing is dispatched.
        state: Whether the schedule fires.
        source: Whether the schedule is static or dynamic.
        created_at: When the schedule was created.
        updated_at: When the schedule was last changed.
    """

    schedule_id: str
    owner_id: str
    project_id: str
    track_id: str
    name: str
    namespace: str
    expression: ScheduleExpression
    timezone: str
    jitter: timedelta | None = None
    target: ScheduleTarget
    state: ScheduleState
    source: ScheduleSource
    created_at: datetime
    updated_at: datetime

    @property
    def is_active(self) -> bool:
        """Whether the schedule fires."""
        return self.state is ScheduleState.ACTIVE


class ScheduleEvent(SchedulesModel, Generic[PayloadT]):
    """One firing of a schedule, as delivered to the target function.

    Attributes:
        schedule_id: Unique identifier of the schedule.
        name: Schedule name, unique within its project and namespace.
        namespace: Namespace the schedule belongs to.
        fired_at: When the schedule fired.
        source: How the schedule was defined, such as `"static"`.
        payload: The payload configured on the schedule, or `None` when the
            dispatch carried no body.
    """

    schedule_id: str
    name: str
    namespace: str
    fired_at: datetime
    source: str
    payload: PayloadT | None = None


JSONValue: TypeAlias = Any
"""Any JSON-serializable value."""


__all__ = [
    "CronExpression",
    "FunctionTarget",
    "JSONValue",
    "OneOffExpression",
    "PayloadT",
    "QueueTarget",
    "Schedule",
    "ScheduleEvent",
    "ScheduleExpression",
    "ScheduleSource",
    "ScheduleState",
    "ScheduleTarget",
]
