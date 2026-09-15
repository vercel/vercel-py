"""Neutral orchestration for Schedules operations.

All business logic lives here, async-only and mode-agnostic, so the sync and
async runtimes share one implementation.
"""

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from vercel.schedules._internal.api_client import (
    SchedulesApiClient,
    SchedulesPage,
    jitter_to_wire,
)
from vercel.schedules._internal.errors import SchedulesValidationError
from vercel.schedules._internal.models import Schedule
from vercel.schedules._internal.options import SchedulesServiceOptions

if TYPE_CHECKING:
    from vercel._internal.core.session import SdkSession, SyncSdkSession


def _require_id(schedule_id: str) -> str:
    if not isinstance(schedule_id, str) or not schedule_id:
        raise SchedulesValidationError("schedule_id must be a non-empty string")
    return schedule_id


def _iso_utc(at: datetime) -> str:
    # Same rendering as JavaScript's `Date.toISOString()`.
    utc = at.astimezone(timezone.utc)
    return utc.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_create_body(
    *,
    topic: str,
    cron: str | None,
    at: datetime | None,
    name: str | None,
    namespace: str | None,
    jitter: timedelta | None,
    payload: Any,
) -> dict[str, Any]:
    """Validate `create_schedule` arguments and render the request body."""
    if not isinstance(topic, str) or not topic:
        raise SchedulesValidationError("topic must be a non-empty string")
    if (cron is None) == (at is None):
        raise SchedulesValidationError("pass exactly one of cron or at")

    body: dict[str, Any] = {}
    if name is not None:
        body["name"] = name
    if namespace is not None:
        body["namespace"] = namespace

    if cron is not None:
        if not isinstance(cron, str) or not cron.strip():
            raise SchedulesValidationError("cron must be a non-empty string")
        body["expression"] = {"type": "cron", "cron": cron}
    else:
        assert at is not None
        if not isinstance(at, datetime):
            raise SchedulesValidationError("at must be a datetime")
        if at.tzinfo is None or at.utcoffset() is None:
            raise SchedulesValidationError(
                "at must be timezone-aware; pass datetime(..., tzinfo=timezone.utc)"
            )
        body["expression"] = {"type": "single", "at": _iso_utc(at)}

    if jitter is not None:
        if not isinstance(jitter, timedelta):
            raise SchedulesValidationError("jitter must be a timedelta")
        if jitter < timedelta(0):
            raise SchedulesValidationError("jitter must not be negative")
        if jitter.microseconds:
            raise SchedulesValidationError("jitter must be a whole number of seconds")
        body["jitter"] = jitter_to_wire(jitter)

    body["target"] = {"type": "queue", "topic": topic}
    if payload is not None:
        body["payload"] = payload
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
        topic: str,
        *,
        cron: str | None,
        at: datetime | None,
        name: str | None,
        namespace: str | None,
        jitter: timedelta | None,
        payload: Any,
    ) -> str:
        self._ensure_open()
        body = build_create_body(
            topic=topic,
            cron=cron,
            at=at,
            name=name,
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
        if page_size is not None and (isinstance(page_size, bool) or page_size < 1):
            raise SchedulesValidationError("page_size must be a positive integer")
        return await self._api_client.list_schedules(
            namespace=namespace, cursor=cursor, limit=page_size
        )

    async def get_schedule(self, schedule_id: str) -> Schedule:
        self._ensure_open()
        return await self._api_client.get_schedule(_require_id(schedule_id))

    async def delete_schedule(self, schedule_id: str) -> None:
        self._ensure_open()
        await self._api_client.delete_schedule(_require_id(schedule_id))

    async def enable_schedule(self, schedule_id: str) -> Schedule:
        self._ensure_open()
        return await self._api_client.enable_schedule(_require_id(schedule_id))

    async def disable_schedule(self, schedule_id: str) -> Schedule:
        self._ensure_open()
        return await self._api_client.disable_schedule(_require_id(schedule_id))


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


__all__ = ["SchedulesService", "build_create_body", "get_schedules_service"]
