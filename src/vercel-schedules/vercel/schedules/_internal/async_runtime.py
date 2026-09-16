"""Async runtime entry points for Schedules operations."""

from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from typing import Any

from vercel.schedules._internal.models import Schedule
from vercel.schedules._internal.sentinel import UNSET
from vercel.schedules._internal.service import SchedulesService


async def create_schedule(
    service: SchedulesService,
    name: str,
    *,
    topic: str,
    cron: str | None = None,
    at: datetime | None = None,
    timezone: str | None = None,
    namespace: str | None = None,
    jitter: timedelta | None = None,
    payload: Any = UNSET,
) -> Schedule:
    return await service.create_schedule(
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


async def get_schedule(
    service: SchedulesService, name: str, *, namespace: str | None = None
) -> Schedule:
    return await service.get_schedule(name, namespace=namespace)


async def update_schedule(
    service: SchedulesService,
    name: str,
    *,
    namespace: str | None = None,
    cron: str | None = None,
    at: datetime | None = None,
    timezone: str | None = None,
    topic: str | None = None,
    jitter: Any = UNSET,
    payload: Any = UNSET,
) -> Schedule:
    return await service.update_schedule(
        name,
        namespace=namespace,
        cron=cron,
        at=at,
        timezone=timezone,
        topic=topic,
        jitter=jitter,
        payload=payload,
    )


async def delete_schedule(
    service: SchedulesService, name: str, *, namespace: str | None = None
) -> None:
    await service.delete_schedule(name, namespace=namespace)


async def enable_schedule(
    service: SchedulesService, name: str, *, namespace: str | None = None
) -> Schedule:
    return await service.enable_schedule(name, namespace=namespace)


async def disable_schedule(
    service: SchedulesService, name: str, *, namespace: str | None = None
) -> Schedule:
    return await service.disable_schedule(name, namespace=namespace)


async def invoke_schedule(
    service: SchedulesService, name: str, *, namespace: str | None = None
) -> None:
    await service.invoke_schedule(name, namespace=namespace)


__all__ = [
    "create_schedule",
    "delete_schedule",
    "disable_schedule",
    "enable_schedule",
    "get_schedule",
    "invoke_schedule",
    "list_schedules",
    "update_schedule",
]
