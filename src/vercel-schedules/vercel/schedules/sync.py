"""Sync mirror for the Vercel Schedules SDK surface.

Every name here matches `vercel.schedules`, with no `await`.

These functions are safe to call from inside a running event loop: the sync
transport never suspends, so nothing deadlocks. They do block the loop for the
duration of the request, so prefer the async surface in async code. Calling them
inside an `async with vercel.api.session(...)` block is rejected outright, since
mixing modes in one session is a bug.

Schedules fire on a cron cadence or once at a fixed instant, and dispatch to a
Vercel Queue topic or function. A schedule is identified by its name within a
namespace. Manage them with `create_schedule` and friends; parse function
dispatches with `parse_schedule_event`.

Requests authenticate with the deployment's Vercel OIDC token. Locally, run
`vercel env pull` or pass `SchedulesServiceOptions(token=...)` in a session.
"""

from collections.abc import Iterator
from datetime import datetime, timedelta

from vercel._internal.core.session import get_active_sync_session
from vercel.schedules._internal.api_client import MAX_JITTER, MIN_JITTER
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
    ScheduleHandler,
    parse_schedule_event,
    resolve_payload_type,
)
from vercel.schedules._internal.models import (
    CronExpression,
    FunctionTarget,
    JSONValue,
    OneOffExpression,
    QueueTarget,
    Schedule,
    ScheduleEvent,
    ScheduleExpression,
    ScheduleSource,
    ScheduleState,
    ScheduleTarget,
)
from vercel.schedules._internal.options import (
    DEFAULT_SCHEDULES_BASE_URL,
    SchedulesCredentialsFactory,
    SchedulesServiceOptions,
)
from vercel.schedules._internal.sentinel import UNSET, _UnsetType
from vercel.schedules._internal.service import SchedulesService, get_schedules_service
from vercel.schedules._internal.sync_runtime import (
    create_schedule as _create_schedule,
    delete_schedule as _delete_schedule,
    disable_schedule as _disable_schedule,
    enable_schedule as _enable_schedule,
    get_schedule as _get_schedule,
    invoke_schedule as _invoke_schedule,
    list_schedules as _list_schedules,
    update_schedule as _update_schedule,
)
from vercel.schedules.version import __version__


def _service() -> SchedulesService:
    return get_schedules_service(get_active_sync_session())


def create_schedule(
    name: str,
    *,
    topic: str,
    cron: str | None = None,
    at: datetime | None = None,
    timezone: str | None = None,
    namespace: str | None = None,
    jitter: timedelta | None = None,
    payload: JSONValue = UNSET,
) -> Schedule:
    """Create a schedule that dispatches to a queue topic.

    Pass exactly one of `cron` (recurring) or `at` (fires once).

    Args:
        name: Schedule name, unique within the project and namespace.
        topic: Queue topic each firing is sent to.
        cron: Five-field cron expression for a recurring schedule, evaluated
            in `timezone`.
        at: When a one-off schedule fires. An aware datetime with a `ZoneInfo`
            (or UTC) supplies the timezone itself; a naive datetime needs
            `timezone`. Given both, `at` is converted into `timezone`.
            Sub-second precision is dropped.
        timezone: IANA timezone name, such as `"America/Los_Angeles"`. The
            service defaults to UTC.
        namespace: Namespace to group schedules under.
        jitter: Random delay added to each firing, in whole minutes, between
            one and fifteen minutes inclusive.
        payload: JSON-serializable value delivered with each firing. Passing
            `None` explicitly configures a JSON `null`; omitting it configures
            no payload.

    Returns:
        The created schedule.

    Raises:
        SchedulesValidationError: If the arguments are inconsistent, such as
            both `cron` and `at`, a naive `at` without `timezone`, or a
            `jitter` outside the allowed range.
        SchedulesApiError: If the service rejects the request.
    """
    return _create_schedule(
        _service(),
        name,
        topic=topic,
        cron=cron,
        at=at,
        timezone=timezone,
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


def get_schedule(name: str, *, namespace: str | None = None) -> Schedule:
    """Fetch one schedule by name.

    Args:
        name: Schedule name.
        namespace: Namespace the schedule lives in; the service default when
            omitted.

    Raises:
        ScheduleNotFoundError: If no schedule has this name in the namespace.
    """
    return _get_schedule(_service(), name, namespace=namespace)


def update_schedule(
    name: str,
    *,
    namespace: str | None = None,
    cron: str | None = None,
    at: datetime | None = None,
    timezone: str | None = None,
    topic: str | None = None,
    jitter: timedelta | None | _UnsetType = UNSET,
    payload: JSONValue = UNSET,
) -> Schedule:
    """Change a schedule's configuration. Omitted fields are left as they are.

    Args:
        name: Schedule name.
        namespace: Namespace the schedule lives in; the service default when
            omitted.
        cron: New cron expression. Mutually exclusive with `at`.
        at: New one-off firing time; same rules as in `create_schedule`.
        timezone: New IANA timezone. Also applied when the expression changes.
        topic: New queue topic to dispatch to.
        jitter: New jitter, between one and fifteen minutes inclusive. Pass
            `None` to remove the jitter.
        payload: New payload. Pass `None` to configure a JSON `null`.

    Returns:
        The updated schedule.

    Raises:
        SchedulesValidationError: If nothing would change, or an argument is
            invalid.
        ScheduleNotFoundError: If no schedule has this name in the namespace.
    """
    return _update_schedule(
        _service(),
        name,
        namespace=namespace,
        cron=cron,
        at=at,
        timezone=timezone,
        topic=topic,
        jitter=jitter,
        payload=payload,
    )


def delete_schedule(name: str, *, namespace: str | None = None) -> None:
    """Delete a schedule. Pending firings are dropped.

    Raises:
        ScheduleNotFoundError: If no schedule has this name in the namespace.
    """
    _delete_schedule(_service(), name, namespace=namespace)


def enable_schedule(name: str, *, namespace: str | None = None) -> Schedule:
    """Resume a disabled schedule.

    Returns:
        The updated schedule.

    Raises:
        ScheduleNotFoundError: If no schedule has this name in the namespace.
    """
    return _enable_schedule(_service(), name, namespace=namespace)


def disable_schedule(name: str, *, namespace: str | None = None) -> Schedule:
    """Pause a schedule without deleting it.

    Returns:
        The updated schedule.

    Raises:
        ScheduleNotFoundError: If no schedule has this name in the namespace.
    """
    return _disable_schedule(_service(), name, namespace=namespace)


def invoke_schedule(name: str, *, namespace: str | None = None) -> None:
    """Fire a schedule now, outside its regular cadence.

    Returns once the service has accepted the firing; the dispatch itself runs
    asynchronously.

    Raises:
        ScheduleNotFoundError: If no schedule has this name in the namespace.
    """
    _invoke_schedule(_service(), name, namespace=namespace)


# Only add public symbols to __all__; internal helpers must stay unexported.
__all__ = [
    "CLOUD_EVENT_ID_HEADER",
    "CLOUD_EVENT_SOURCE_HEADER",
    "CLOUD_EVENT_SPEC_VERSION_HEADER",
    "CLOUD_EVENT_TYPE_HEADER",
    "DEFAULT_SCHEDULES_BASE_URL",
    "MAX_JITTER",
    "MIN_JITTER",
    "SCHEDULE_CLOUD_EVENT_TYPE",
    "SCHEDULE_FIRED_AT_HEADER",
    "SCHEDULE_ID_HEADER",
    "SCHEDULE_NAMESPACE_HEADER",
    "SCHEDULE_NAME_HEADER",
    "SCHEDULE_SOURCE_HEADER",
    "UNSET",
    "CronExpression",
    "FunctionTarget",
    "JSONValue",
    "OneOffExpression",
    "QueueTarget",
    "Schedule",
    "ScheduleEvent",
    "ScheduleEventParseError",
    "ScheduleExpression",
    "ScheduleHandler",
    "ScheduleNotFoundError",
    "ScheduleSource",
    "ScheduleState",
    "ScheduleTarget",
    "SchedulesApiError",
    "SchedulesCredentialsError",
    "SchedulesCredentialsFactory",
    "SchedulesError",
    "SchedulesResponseError",
    "SchedulesServiceOptions",
    "SchedulesValidationError",
    "__version__",
    "create_schedule",
    "delete_schedule",
    "disable_schedule",
    "enable_schedule",
    "get_schedule",
    "invoke_schedule",
    "list_schedules",
    "parse_schedule_event",
    "resolve_payload_type",
    "update_schedule",
]
