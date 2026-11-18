"""Neutral orchestration for Schedules operations.

All business logic lives here, async-only and mode-agnostic, so the sync and
async runtimes share one implementation.
"""

from collections.abc import Callable
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from vercel._internal.core.sentinel import UNSET, UnsetType
from vercel.schedules._internal.api_client import SchedulesApiClient, SchedulesPage
from vercel.schedules._internal.errors import SchedulesValidationError
from vercel.schedules._internal.models import (
    JITTER_UNIT,
    MAX_JITTER,
    MIN_JITTER,
    Cron,
    JSONValue,
    OneOffExpression,
    Schedule,
    ScheduleExpression,
)
from vercel.schedules._internal.options import (
    SchedulesCredentialsFactory,
    SchedulesServiceOptions,
    SyncSchedulesServiceOptions,
    adapt_sync_credentials_factory,
)

if TYPE_CHECKING:
    from vercel._internal.core.session import SdkSession, SyncSdkSession

UTC_NAME = "UTC"


def _require_non_empty(value: str, label: str) -> str:
    if not value.strip():
        raise SchedulesValidationError(f"{label} must be a non-empty string")
    return value


def _check_name(name: str) -> str:
    # URL parsing drops an empty path segment and resolves `.` and `..`, so these
    # names would reach the list route or another resource instead.
    if name.strip() in ("", ".", ".."):
        raise SchedulesValidationError(f"name {name!r} is not a valid schedule name")
    return name


def _check_namespace(namespace: str | None) -> None:
    if namespace is not None:
        _require_non_empty(namespace, "namespace")


def zone_info(timezone: str) -> ZoneInfo:
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


def _round_up_to_minute(at: datetime) -> datetime:
    """One-off schedules have minute precision. Rounding up rather than down means
    the firing never happens before `at`, and an `at` in the future never becomes
    one in the past.
    """
    if at.second or at.microsecond:
        return at.replace(second=0, microsecond=0) + timedelta(minutes=1)
    return at


def resolve_one_off(at: datetime, timezone: str | None) -> tuple[datetime, str]:
    """Return the wall-clock time and IANA timezone of a one-off schedule.

    The service stores one-off firings as a wall-clock time in a named
    timezone. An aware `at` carrying a `ZoneInfo` (or UTC) supplies that name
    itself; a naive `at` needs an explicit `timezone`. When both are given,
    `at` is converted into `timezone`, so the instant is preserved.
    """
    if at.tzinfo is None or at.utcoffset() is None:
        if timezone is None:
            raise SchedulesValidationError(
                "at is naive; pass timezone=... or make it timezone-aware with ZoneInfo"
            )
        return _round_up_to_minute(at), _require_non_empty(timezone, "timezone")

    if timezone is not None:
        return _round_up_to_minute(at.astimezone(zone_info(timezone))), timezone

    name = _timezone_name(at)
    if name is None:
        raise SchedulesValidationError(
            "at has a fixed offset but no IANA timezone; use ZoneInfo or pass timezone=..."
        )
    return _round_up_to_minute(at), name


def _resolve_expression(
    cron: str | Cron | None, at: datetime | None, timezone: str | None
) -> tuple[ScheduleExpression | None, str | None]:
    """The expression to send, if any, and the `timezone` sent with it.

    A cron, or a timezone on its own, passes the timezone name through for the
    service to check. A one-off always resolves one: its wall-clock time means
    nothing without the zone it is in.
    """
    if at is not None:
        wall_clock, zone = resolve_one_off(at, timezone)
        return OneOffExpression(at=wall_clock), zone
    if timezone is not None:
        _require_non_empty(timezone, "timezone")
    if isinstance(cron, str):
        cron = Cron.from_crontab(cron)
    return cron, timezone


def _check_until(until: datetime | None) -> None:
    if until is not None and until.utcoffset() is None:
        raise SchedulesValidationError("until is naive; make it timezone-aware")


def check_jitter(jitter: object) -> None:
    if not isinstance(jitter, timedelta):
        raise SchedulesValidationError(
            f"jitter must be a timedelta, such as timedelta(minutes=5), got {jitter!r}"
        )
    if not MIN_JITTER <= jitter <= MAX_JITTER:
        raise SchedulesValidationError(
            f"jitter must be between {MIN_JITTER} and {MAX_JITTER} inclusive, got {jitter}"
        )
    if jitter % JITTER_UNIT:
        raise SchedulesValidationError("jitter must be a whole number of minutes")


class SchedulesService:
    """Orchestrates schedule management against the API client."""

    def __init__(
        self,
        *,
        api_client: SchedulesApiClient,
        ensure_open: Callable[[], None],
    ) -> None:
        self._api_client = api_client
        self._ensure_open = ensure_open

    async def create_schedule(
        self,
        name: str,
        *,
        topic: str,
        cron: str | Cron | None,
        at: datetime | None,
        timezone: str | None,
        namespace: str | None,
        jitter: timedelta | None,
        payload: JSONValue | UnsetType,
    ) -> Schedule:
        self._ensure_open()
        _check_name(name)
        _check_namespace(namespace)
        _require_non_empty(topic, "topic")
        if cron is not None and at is not None:
            raise SchedulesValidationError("pass exactly one of cron or at")
        expression, timezone = _resolve_expression(cron, at, timezone)
        if expression is None:
            raise SchedulesValidationError("pass exactly one of cron or at")
        if jitter is not None:
            check_jitter(jitter)
        return await self._api_client.create_schedule(
            name=name,
            namespace=namespace,
            expression=expression,
            timezone=timezone,
            jitter=jitter,
            topic=topic,
            payload=payload,
        )

    async def query_schedules_page(
        self,
        *,
        namespace: str | None,
        cursor: str | None,
        page_size: int | None,
    ) -> SchedulesPage:
        self._ensure_open()
        _check_namespace(namespace)
        if page_size is not None and page_size < 1:
            raise SchedulesValidationError("page_size must be a positive integer")
        return await self._api_client.query_schedules_page(
            namespace=namespace, cursor=cursor, limit=page_size
        )

    async def get_schedule(self, name: str, *, namespace: str | None) -> Schedule:
        self._ensure_open()
        _check_namespace(namespace)
        return await self._api_client.get_schedule(_check_name(name), namespace=namespace)

    async def update_schedule(
        self,
        name: str,
        *,
        namespace: str | None,
        cron: str | Cron | None,
        at: datetime | None,
        timezone: str | None,
        topic: str | None,
        jitter: timedelta | None | UnsetType,
        payload: JSONValue | UnsetType,
    ) -> Schedule:
        """Change only the supplied fields.

        `jitter=None` and `payload=None` remove them; omitting them leaves them
        unchanged. `timezone` goes with whichever expression is being set; on
        its own it re-zones the existing expression.
        """
        self._ensure_open()
        _check_name(name)
        _check_namespace(namespace)
        if cron is not None and at is not None:
            raise SchedulesValidationError("pass at most one of cron or at")
        expression, timezone = _resolve_expression(cron, at, timezone)
        if topic is not None:
            _require_non_empty(topic, "topic")
        if not (jitter is None or isinstance(jitter, UnsetType)):
            check_jitter(jitter)
        unchanged = expression is None and timezone is None and topic is None
        if unchanged and jitter is UNSET and payload is UNSET:
            raise SchedulesValidationError("update_schedule needs at least one field to change")
        return await self._api_client.update_schedule(
            name,
            namespace=namespace,
            expression=expression,
            timezone=timezone,
            topic=topic,
            jitter=jitter,
            payload=payload,
        )

    async def delete_schedule(self, name: str, *, namespace: str | None) -> None:
        self._ensure_open()
        _check_namespace(namespace)
        await self._api_client.delete_schedule(_check_name(name), namespace=namespace)

    async def enable_schedule(
        self, name: str, *, namespace: str | None, until: datetime | None
    ) -> Schedule:
        self._ensure_open()
        _check_namespace(namespace)
        _check_until(until)
        return await self._api_client.enable_schedule(
            _check_name(name), namespace=namespace, until=until
        )

    async def disable_schedule(self, name: str, *, namespace: str | None) -> Schedule:
        self._ensure_open()
        _check_namespace(namespace)
        return await self._api_client.disable_schedule(_check_name(name), namespace=namespace)

    async def invoke_schedule(self, name: str, *, namespace: str | None) -> None:
        self._ensure_open()
        _check_namespace(namespace)
        await self._api_client.invoke_schedule(_check_name(name), namespace=namespace)


def _create_service(
    session: "SdkSession | SyncSdkSession",
    options: SchedulesServiceOptions | SyncSchedulesServiceOptions,
    credentials_factory: SchedulesCredentialsFactory,
) -> SchedulesService:
    return SchedulesService(
        api_client=SchedulesApiClient(
            base_url=options.base_url,
            credentials_factory=credentials_factory,
            transport=session.get_transport(),
            timeout=options.timeout,
        ),
        ensure_open=session.check_open,
    )


def get_schedules_service(session: "SdkSession") -> SchedulesService:
    """Resolve the Schedules service for an async session, once per session."""

    def factory() -> SchedulesService:
        options = session.get_service_option(SchedulesServiceOptions) or SchedulesServiceOptions()
        return _create_service(session, options, options.resolve_credentials_factory())

    return session.get_or_create_service(SchedulesService, factory)


def get_sync_schedules_service(session: "SyncSdkSession") -> SchedulesService:
    """Resolve the Schedules service for a sync session, once per session."""

    def factory() -> SchedulesService:
        options = (
            session.get_service_option(SyncSchedulesServiceOptions) or SyncSchedulesServiceOptions()
        )
        credentials_factory = adapt_sync_credentials_factory(options.resolve_credentials_factory())
        return _create_service(session, options, credentials_factory)

    return session.get_or_create_service(SchedulesService, factory)


__all__ = [
    "SchedulesService",
    "get_schedules_service",
    "get_sync_schedules_service",
    "resolve_one_off",
]
