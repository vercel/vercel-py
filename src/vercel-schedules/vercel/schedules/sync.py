"""Sync mirror for the Vercel Schedules SDK surface.

Every name here matches `vercel.schedules`, with no `await`.

These functions are safe to call from inside a running event loop: the sync
transport never suspends, so nothing deadlocks. They do block the loop for the
duration of the request, so prefer the async surface in async code. Calling them
inside an `async with vercel.api.session(...)` block is rejected outright, since
mixing modes in one session is a bug.

Schedules fire on a cron cadence or once at a fixed instant, and dispatch to a
Vercel Queue topic. Manage them with `create_schedule` and friends; receive
firings in a function with `schedule_handler` or `parse_schedule_event`.

Requests authenticate with the deployment's Vercel OIDC token. Locally, run
`vercel env pull` or pass `SchedulesServiceOptions(token=...)` in a session.
"""

from collections.abc import Iterator
from datetime import datetime, timedelta

from vercel._internal.core.session import get_active_sync_session
from vercel.schedules._internal.asgi import (
    ScheduleAsgiApp,
    ScheduleHandler,
    schedule_handler,
)
from vercel.schedules._internal.errors import (
    ScheduleEventParseError,
    ScheduleNotFoundError,
    SchedulesApiError,
    SchedulesCredentialsError,
    SchedulesError,
    SchedulesResponseError,
    SchedulesValidationError,
)
from vercel.schedules._internal.events import (
    CLOUD_EVENT_ID_HEADER,
    CLOUD_EVENT_SOURCE_HEADER,
    CLOUD_EVENT_SPEC_VERSION_HEADER,
    CLOUD_EVENT_TYPE_HEADER,
    SCHEDULE_CLOUD_EVENT_TYPE,
    SCHEDULE_FIRED_AT_HEADER,
    SCHEDULE_ID_HEADER,
    SCHEDULE_NAME_HEADER,
    SCHEDULE_NAMESPACE_HEADER,
    SCHEDULE_SOURCE_HEADER,
    parse_schedule_event,
)
from vercel.schedules._internal.models import (
    CronExpression,
    JSONValue,
    OneOffExpression,
    QueueTarget,
    Schedule,
    ScheduleEvent,
    ScheduleExpression,
    ScheduleSource,
    ScheduleState,
    StateOverride,
)
from vercel.schedules._internal.options import (
    DEFAULT_SCHEDULES_BASE_URL,
    SchedulesCredentialsFactory,
    SchedulesServiceOptions,
)
from vercel.schedules._internal.sentinel import UNSET
from vercel.schedules._internal.service import SchedulesService, get_schedules_service
from vercel.schedules._internal.sync_runtime import (
    create_schedule as _create_schedule,
    delete_schedule as _delete_schedule,
    disable_schedule as _disable_schedule,
    enable_schedule as _enable_schedule,
    get_schedule as _get_schedule,
    list_schedules as _list_schedules,
)
from vercel.schedules.version import __version__


def _service() -> SchedulesService:
    return get_schedules_service(get_active_sync_session())


def create_schedule(
    topic: str,
    *,
    cron: str | None = None,
    at: datetime | None = None,
    name: str | None = None,
    namespace: str | None = None,
    jitter: timedelta | None = None,
    payload: JSONValue = UNSET,
) -> str:
    """Create a schedule that dispatches to a queue topic.

    Pass exactly one of `cron` (recurring) or `at` (fires once).

    Args:
        topic: Queue topic each firing is sent to.
        cron: Five-field cron expression for a recurring schedule.
        at: Timezone-aware instant for a schedule that fires once.
        name: Schedule name, unique within the project and namespace. The
            service assigns one when omitted.
        namespace: Namespace to group schedules under.
        jitter: Random delay added to each firing, in whole seconds.
        payload: JSON-serializable value delivered with each firing. Passing
            `None` explicitly configures a JSON `null`; omitting it configures
            no payload.

    Returns:
        The new schedule's id.

    Raises:
        SchedulesValidationError: If the arguments are inconsistent, such as
            both `cron` and `at`, a naive `at`, or a sub-second `jitter`.
        SchedulesApiError: If the service rejects the request.
    """
    return _create_schedule(
        _service(),
        topic,
        cron=cron,
        at=at,
        name=name,
        namespace=namespace,
        jitter=jitter,
        payload=payload,
    )


def list_schedules(
    *,
    namespace: str | None = None,
    page_size: int | None = None,
) -> Iterator[Schedule]:
    """Iterate over the project's schedules.

    Args:
        namespace: Only schedules in this namespace.
        page_size: Maximum number of schedules fetched per API request.

    Returns:
        An iterator that transparently follows pagination cursors.
    """
    return _list_schedules(_service(), namespace=namespace, page_size=page_size)


def get_schedule(schedule_id: str) -> Schedule:
    """Fetch one schedule by id.

    Raises:
        ScheduleNotFoundError: If no schedule has this id.
    """
    return _get_schedule(_service(), schedule_id)


def delete_schedule(schedule_id: str) -> None:
    """Delete a schedule. Pending firings are dropped.

    Raises:
        ScheduleNotFoundError: If no schedule has this id.
    """
    _delete_schedule(_service(), schedule_id)


def enable_schedule(schedule_id: str) -> Schedule:
    """Resume a disabled schedule.

    Returns:
        The updated schedule.

    Raises:
        ScheduleNotFoundError: If no schedule has this id.
    """
    return _enable_schedule(_service(), schedule_id)


def disable_schedule(schedule_id: str) -> Schedule:
    """Pause a schedule without deleting it.

    Returns:
        The updated schedule.

    Raises:
        ScheduleNotFoundError: If no schedule has this id.
    """
    return _disable_schedule(_service(), schedule_id)


# Only add public symbols to __all__; internal helpers must stay unexported.
__all__ = [
    "CLOUD_EVENT_ID_HEADER",
    "CLOUD_EVENT_SOURCE_HEADER",
    "CLOUD_EVENT_SPEC_VERSION_HEADER",
    "CLOUD_EVENT_TYPE_HEADER",
    "DEFAULT_SCHEDULES_BASE_URL",
    "SCHEDULE_CLOUD_EVENT_TYPE",
    "SCHEDULE_FIRED_AT_HEADER",
    "SCHEDULE_ID_HEADER",
    "SCHEDULE_NAMESPACE_HEADER",
    "SCHEDULE_NAME_HEADER",
    "SCHEDULE_SOURCE_HEADER",
    "CronExpression",
    "JSONValue",
    "OneOffExpression",
    "QueueTarget",
    "Schedule",
    "ScheduleAsgiApp",
    "ScheduleEvent",
    "ScheduleEventParseError",
    "ScheduleExpression",
    "ScheduleHandler",
    "ScheduleNotFoundError",
    "ScheduleSource",
    "ScheduleState",
    "SchedulesApiError",
    "SchedulesCredentialsError",
    "SchedulesCredentialsFactory",
    "SchedulesError",
    "SchedulesResponseError",
    "SchedulesServiceOptions",
    "SchedulesValidationError",
    "StateOverride",
    "__version__",
    "create_schedule",
    "delete_schedule",
    "disable_schedule",
    "enable_schedule",
    "get_schedule",
    "list_schedules",
    "parse_schedule_event",
    "schedule_handler",
]
