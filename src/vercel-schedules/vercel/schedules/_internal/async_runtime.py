"""Async runtime entry points for Schedules operations."""

from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from typing import Any

from vercel.schedules._internal.models import Schedule
from vercel.schedules._internal.sentinel import UNSET
from vercel.schedules._internal.service import SchedulesService


async def create_schedule(
    service: SchedulesService,
    topic: str,
    *,
    cron: str | None = None,
    at: datetime | None = None,
    name: str | None = None,
    namespace: str | None = None,
    jitter: timedelta | None = None,
    payload: Any = UNSET,
) -> str:
    return await service.create_schedule(
        topic,
        cron=cron,
        at=at,
        name=name,
        namespace=namespace,
        jitter=jitter,
        payload=payload,
    )


def list_schedules(
    service: SchedulesService,
    *,
    namespace: str | None = None,
    page_size: int | None = None,
) -> AsyncIterator[Schedule]:
    async def iterate() -> AsyncIterator[Schedule]:
        cursor: str | None = None
        while True:
            page = await service.list_schedules_page(
                namespace=namespace, cursor=cursor, page_size=page_size
            )
            for schedule in page.schedules:
                yield schedule
            if page.next_cursor is None or not page.schedules:
                return
            cursor = page.next_cursor

    return iterate()


async def get_schedule(service: SchedulesService, schedule_id: str) -> Schedule:
    return await service.get_schedule(schedule_id)


async def delete_schedule(service: SchedulesService, schedule_id: str) -> None:
    await service.delete_schedule(schedule_id)


async def enable_schedule(service: SchedulesService, schedule_id: str) -> Schedule:
    return await service.enable_schedule(schedule_id)


async def disable_schedule(service: SchedulesService, schedule_id: str) -> Schedule:
    return await service.disable_schedule(schedule_id)


__all__ = [
    "create_schedule",
    "delete_schedule",
    "disable_schedule",
    "enable_schedule",
    "get_schedule",
    "list_schedules",
]
