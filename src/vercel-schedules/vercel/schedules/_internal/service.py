"""Neutral orchestration for Schedules operations.

All business logic lives here, async-only and mode-agnostic, so the sync and
async runtimes share one implementation.
"""

from collections.abc import Callable
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from vercel.schedules._internal.api_client import (
    MAX_JITTER,
    MIN_JITTER,
    SchedulesApiClient,
    SchedulesPage,
    jitter_to_wire,
)
from vercel.schedules._internal.errors import SchedulesValidationError
from vercel.schedules._internal.models import Schedule
from vercel.schedules._internal.options import SchedulesServiceOptions
from vercel.schedules._internal.sentinel import UNSET

if TYPE_CHECKING:
    from vercel._internal.core.session import SdkSession, SyncSdkSession

UTC_NAME = "UTC"


def _require_name(name: str) -> str:
    if not isinstance(name, str) or not name:
        raise SchedulesValidationError("name must be a non-empty string")
    return name


def _check_namespace(namespace: str | None) -> None:
    if namespace is not None and (not isinstance(namespace, str) or not namespace):
        raise SchedulesValidationError("namespace must be a non-empty string")


def _check_topic(topic: str) -> None:
    if not isinstance(topic, str) or not topic:
        raise SchedulesValidationError("topic must be a non-empty string")


def _check_cron(cron: str) -> None:
    if not isinstance(cron, str) or not cron.strip():
        raise SchedulesValidationError("cron must be a non-empty string")


def _check_timezone(timezone: str) -> None:
    if not isinstance(timezone, str) or not timezone:
        raise SchedulesValidationError("timezone must be a non-empty IANA timezone name")


def _zone(timezone: str) -> ZoneInfo:
    try:
        return ZoneInfo(timezone)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise SchedulesValidationError(
            f"timezone {timezone!r} is not a known IANA timezone"
        ) from exc


def _timezone_name(at: datetime) -> str | None:
    """The IANA name carried by an aware datetime, or `None` if it has none."""
    tzinfo = at.tzinfo
    if tzinfo is None:
        return None
    key = getattr(tzinfo, "key", None)
    if isinstance(key, str) and key:
        return key
    if at.utcoffset() == timedelta(0):
        return UTC_NAME
    return None


def _wall_clock(at: datetime) -> str:
    """Render a wall-clock time the way the service expects: no offset, whole seconds."""
    return at.replace(tzinfo=None, microsecond=0).isoformat(timespec="seconds")


def resolve_one_off(at: datetime, timezone: str | None) -> tuple[str, str]:
    """Return the wire `at` and IANA `timezone` for a one-off schedule.

    The service stores one-off firings as a wall-clock time in a named
    timezone. An aware `at` carrying a `ZoneInfo` (or UTC) supplies that name
    itself; a naive `at` needs an explicit `timezone`. When both are given,
    `at` is converted into `timezone`, so the instant is preserved.
    """
    if not isinstance(at, datetime):
        raise SchedulesValidationError("at must be a datetime")

    if at.tzinfo is None or at.utcoffset() is None:
        if timezone is None:
            raise SchedulesValidationError(
                "at is naive; pass timezone=... or make it timezone-aware with ZoneInfo"
            )
        _check_timezone(timezone)
        return _wall_clock(at), timezone

    if timezone is not None:
        _check_timezone(timezone)
        return _wall_clock(at.astimezone(_zone(timezone))), timezone

    name = _timezone_name(at)
    if name is None:
        raise SchedulesValidationError(
            "at has a fixed offset but no IANA timezone; use ZoneInfo or pass timezone=..."
        )
    return _wall_clock(at), name


def _check_jitter(jitter: timedelta) -> None:
    if not isinstance(jitter, timedelta):
        raise SchedulesValidationError("jitter must be a timedelta")
    if not MIN_JITTER <= jitter <= MAX_JITTER:
        raise SchedulesValidationError(
            f"jitter must be between {MIN_JITTER} and {MAX_JITTER} inclusive, got {jitter}"
        )
    if jitter % timedelta(minutes=1):
        raise SchedulesValidationError("jitter must be a whole number of minutes")


def _expression(cron: str | None, at: datetime | None, timezone: str | None) -> dict[str, Any]:
    """Render `expression` and, when derived from `at`, `timezone`."""
    body: dict[str, Any] = {}
    if cron is not None:
        _check_cron(cron)
        body["expression"] = {"type": "cron", "cron": cron}
        if timezone is not None:
            _check_timezone(timezone)
            body["timezone"] = timezone
    else:
        assert at is not None
        wire_at, zone_name = resolve_one_off(at, timezone)
        body["expression"] = {"type": "single", "at": wire_at}
        body["timezone"] = zone_name
    return body


def build_create_body(
    *,
    name: str,
    topic: str,
    cron: str | None,
    at: datetime | None,
    timezone: str | None,
    namespace: str | None,
    jitter: timedelta | None,
    payload: Any,
) -> dict[str, Any]:
    """Validate `create_schedule` arguments and render the request body."""
    _require_name(name)
    _check_namespace(namespace)
    _check_topic(topic)
    if (cron is None) == (at is None):
        raise SchedulesValidationError("pass exactly one of cron or at")

    body: dict[str, Any] = {"name": name}
    if namespace is not None:
        body["namespace"] = namespace
    body.update(_expression(cron, at, timezone))
    if jitter is not None:
        _check_jitter(jitter)
        body["jitter"] = jitter_to_wire(jitter)
    body["target"] = {"type": "queue", "topic": topic}
    if payload is not UNSET:
        body["payload"] = payload
    return body


def build_update_body(
    *,
    cron: str | None,
    at: datetime | None,
    timezone: str | None,
    topic: str | None,
    jitter: Any,
    payload: Any,
) -> dict[str, Any]:
    """Validate `update_schedule` arguments and render the PATCH body.

    Only supplied fields are sent. `jitter=None` clears the jitter; omitting it
    leaves it unchanged.
    """
    if cron is not None and at is not None:
        raise SchedulesValidationError("pass at most one of cron or at")

    body: dict[str, Any] = {}
    if cron is not None or at is not None:
        body.update(_expression(cron, at, timezone))
    elif timezone is not None:
        _check_timezone(timezone)
        body["timezone"] = timezone

    if topic is not None:
        _check_topic(topic)
        body["target"] = {"type": "queue", "topic": topic}

    if jitter is None:
        body["jitter"] = None
    elif jitter is not UNSET:
        _check_jitter(jitter)
        body["jitter"] = jitter_to_wire(jitter)

    if payload is not UNSET:
        body["payload"] = payload

    if not body:
        raise SchedulesValidationError("update_schedule needs at least one field to change")
    return body


class SchedulesService:
    """Orchestrates schedule management against the API client."""

    def __init__(
        self,
        *,
        api_client: SchedulesApiClient,
        options: SchedulesServiceOptions,
        ensure_open: Callable[[], None],
    ) -> None:
        self._api_client = api_client
        self._options = options
        self._ensure_open = ensure_open

    @property
    def options(self) -> SchedulesServiceOptions:
        return self._options

    async def create_schedule(
        self,
        name: str,
        *,
        topic: str,
        cron: str | None,
        at: datetime | None,
        timezone: str | None,
        namespace: str | None,
        jitter: timedelta | None,
        payload: Any,
    ) -> Schedule:
        self._ensure_open()
        body = build_create_body(
            name=name,
            topic=topic,
            cron=cron,
            at=at,
            timezone=timezone,
            namespace=namespace,
            jitter=jitter,
            payload=payload,
        )
        return await self._api_client.create_schedule(body)

    async def list_schedules_page(
        self,
        *,
        namespace: str | None,
        cursor: str | None,
        page_size: int | None,
    ) -> SchedulesPage:
        self._ensure_open()
        _check_namespace(namespace)
        if page_size is not None and (
            isinstance(page_size, bool) or not isinstance(page_size, int) or page_size < 1
        ):
            raise SchedulesValidationError("page_size must be a positive integer")
        return await self._api_client.list_schedules(
            namespace=namespace, cursor=cursor, limit=page_size
        )

    async def get_schedule(self, name: str, *, namespace: str | None) -> Schedule:
        self._ensure_open()
        _check_namespace(namespace)
        return await self._api_client.get_schedule(_require_name(name), namespace=namespace)

    async def update_schedule(
        self,
        name: str,
        *,
        namespace: str | None,
        cron: str | None,
        at: datetime | None,
        timezone: str | None,
        topic: str | None,
        jitter: Any,
        payload: Any,
    ) -> Schedule:
        self._ensure_open()
        _require_name(name)
        _check_namespace(namespace)
        body = build_update_body(
            cron=cron, at=at, timezone=timezone, topic=topic, jitter=jitter, payload=payload
        )
        return await self._api_client.update_schedule(name, namespace=namespace, body=body)

    async def delete_schedule(self, name: str, *, namespace: str | None) -> None:
        self._ensure_open()
        _check_namespace(namespace)
        await self._api_client.delete_schedule(_require_name(name), namespace=namespace)

    async def enable_schedule(self, name: str, *, namespace: str | None) -> Schedule:
        self._ensure_open()
        _check_namespace(namespace)
        return await self._api_client.enable_schedule(_require_name(name), namespace=namespace)

    async def disable_schedule(self, name: str, *, namespace: str | None) -> Schedule:
        self._ensure_open()
        _check_namespace(namespace)
        return await self._api_client.disable_schedule(_require_name(name), namespace=namespace)

    async def invoke_schedule(self, name: str, *, namespace: str | None) -> None:
        self._ensure_open()
        _check_namespace(namespace)
        await self._api_client.invoke_schedule(_require_name(name), namespace=namespace)


def get_schedules_service(session: "SdkSession | SyncSdkSession") -> SchedulesService:
    """Resolve the Schedules service for a session, creating it once per session."""
    from vercel._internal.core.session import SyncSdkSession

    def factory() -> SchedulesService:
        options = session.get_service_option(SchedulesServiceOptions) or SchedulesServiceOptions()
        is_sync = isinstance(session, SyncSdkSession)
        return SchedulesService(
            api_client=SchedulesApiClient(
                base_url=options.base_url,
                credentials_factory=options.resolve_credentials_factory(sync=is_sync),
                transport=session.get_transport(),
                timeout=options.timeout,
            ),
            options=options,
            ensure_open=session.check_open,
        )

    return session.get_or_create_service(SchedulesService, factory)


__all__ = [
    "SchedulesService",
    "build_create_body",
    "build_update_body",
    "get_schedules_service",
    "resolve_one_off",
]
