"""Async runtime entry points for Drive operations."""

from collections.abc import AsyncIterator
from typing import Self

from vercel._internal.core.region import RegionInput, resolve_region
from vercel.drive._internal.handle import DriveHandleBase
from vercel.drive._internal.models import DriveQuery, QueryDrivesPage, QueryDrivesParams
from vercel.drive._internal.service import DriveService


class Drive(DriveHandleBase):
    """Operate on a project-local persistent Drive."""

    __slots__ = ()

    async def fork(self, *, name: str) -> "Drive":
        """Fork this Drive's committed data, inheriting its region and maximum size.

        Return a new handle using the same service without changing this Drive.
        API failures propagate as DriveApiError, including missing source
        (404) and conflicting or uninitialized Drives (409).
        """
        return await fork_drive(self._service, source=self.name, name=name)

    async def delete(self) -> Self:
        """Delete by project and name, then refresh this handle from the response.

        Deletion does not use the Drive ID. If the name now refers to a replacement
        Drive, this method deletes that replacement and updates this handle to it.
        """
        payload = await self._service.delete_drive(name=self.name)
        self._apply_payload(payload)
        return self


async def fork_drive(service: DriveService, *, source: str, name: str) -> Drive:
    return Drive(
        payload=await service.fork_drive(name=source, fork_name=name),
        service=service,
    )


async def get_drive(service: DriveService, *, name_or_id: str) -> Drive:
    return Drive(
        payload=await service.get_drive(name_or_id=name_or_id),
        service=service,
    )


async def get_or_create_drive(
    service: DriveService,
    *,
    name: str,
    max_size_bytes: int | None = None,
    region: RegionInput = None,
) -> tuple[Drive, bool]:
    state, created = await service.get_or_create_drive(
        name=name,
        max_size_bytes=max_size_bytes,
        region=resolve_region(region, fallback=service.region),
    )
    return Drive(payload=state, service=service), created


async def delete_drive(service: DriveService, *, name: str) -> Drive:
    return Drive(
        payload=await service.delete_drive(name=name),
        service=service,
    )


async def query_drives_page(
    service: DriveService,
    *,
    query: DriveQuery | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
) -> QueryDrivesPage[Drive]:
    page = await service.query_drives_page(query=query, page_size=page_size, cursor=cursor)
    return QueryDrivesPage(
        drives=[Drive(payload=state, service=service) for state in page.drives],
        next_cursor=page.next_cursor,
    )


def query_drives(
    service: DriveService,
    *,
    query: DriveQuery | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
) -> AsyncIterator[Drive]:
    async def iterate() -> AsyncIterator[Drive]:
        params = QueryDrivesParams(page_size=page_size, cursor=cursor)
        while True:
            page = await query_drives_page(
                service,
                query=query,
                page_size=params.page_size,
                cursor=params.cursor,
            )
            for drive in page.drives:
                yield drive
            if page.next_cursor is None or not page.drives:
                return
            params = params.with_cursor(page.next_cursor)

    return iterate()


__all__ = [
    "Drive",
    "delete_drive",
    "fork_drive",
    "get_drive",
    "get_or_create_drive",
    "query_drives",
    "query_drives_page",
]
