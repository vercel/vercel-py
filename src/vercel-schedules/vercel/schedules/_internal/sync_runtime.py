"""Sync runtime entry points for Schedules operations.

Each call steps the async service exactly once through `iter_coroutine`, which is
valid because the sync transport never suspends.
"""

from collections.abc import Iterator
from datetime import datetime, timedelta
from typing import Any

from vercel._internal.core.iter_coroutine import iter_coroutine
from vercel.schedules._internal.models import Schedule
from vercel.schedules._internal.service import SchedulesService


def create_schedule(
    service: SchedulesService,
    topic: str,
    *,
    cron: str | None = None,
    at: datetime | None = None,
    name: str | None = None,
    namespace: str | None = None,
    jitter: timedelta | None = None,
    payload: Any = None,
) -> str:
    return iter_coroutine(
        service.create_schedule(
            topic,
            cron=cron,
            at=at,
            name=name,
            namespace=namespace,
            jitter=jitter,
            payload=payload,
        )
    )


def list_schedules(
    service: SchedulesService,
    *,
    namespace: str | None = None,
    page_size: int | None = None,
) -> Iterator[Schedule]:
    cursor: str | None = None
    while True:
        page = iter_coroutine(
            service.list_schedules_page(namespace=namespace, cursor=cursor, page_size=page_size)
        )
        yield from page.schedules
        if page.next_cursor is None or not page.schedules:
            return
        cursor = page.next_cursor


def get_schedule(service: SchedulesService, schedule_id: str) -> Schedule:
    return iter_coroutine(service.get_schedule(schedule_id))


def delete_schedule(service: SchedulesService, schedule_id: str) -> None:
    iter_coroutine(service.delete_schedule(schedule_id))


def enable_schedule(service: SchedulesService, schedule_id: str) -> Schedule:
    return iter_coroutine(service.enable_schedule(schedule_id))


def disable_schedule(service: SchedulesService, schedule_id: str) -> Schedule:
    return iter_coroutine(service.disable_schedule(schedule_id))


__all__ = [
    "create_schedule",
    "delete_schedule",
    "disable_schedule",
    "enable_schedule",
    "get_schedule",
    "list_schedules",
]
