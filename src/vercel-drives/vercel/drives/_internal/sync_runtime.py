"""Sync runtime entry points for Drive operations.

Each call steps the async service exactly once through `iter_coroutine`, which is
valid because the sync transport never suspends.
"""

from collections.abc import Iterator
from typing import Self

from vercel._internal.core.iter_coroutine import iter_coroutine
from vercel._internal.core.region import RegionInput, resolve_region
from vercel.drives._internal.handle import DriveHandleBase
from vercel.drives._internal.models import DriveQuery, QueryDrivesPage, QueryDrivesParams
from vercel.drives._internal.service import DriveService


class SyncDrive(DriveHandleBase):
    """Operate synchronously on a project-local persistent Drive."""

    __slots__ = ()

    def fork(self, *, name: str) -> "SyncDrive":
        """Fork this Drive's committed data, inheriting its region and maximum size.

        Return a new handle using the same service without changing this Drive.
        API failures propagate as DriveApiError, including missing source
        (404) and conflicting or uninitialized Drives (409).
        """
        return fork_drive(self._service, source=self.name, name=name)

    def delete(self) -> Self:
        """Delete by project and name, then refresh this handle from the response.

        Deletion does not use the Drive ID. If the name now refers to a replacement
        Drive, this method deletes that replacement and updates this handle to it.
        """
        payload = iter_coroutine(self._service.delete_drive(name=self.name))
        self._apply_payload(payload)
        return self


def fork_drive(service: DriveService, *, source: str, name: str) -> SyncDrive:
    return SyncDrive(
        payload=iter_coroutine(service.fork_drive(name=source, fork_name=name)),
        service=service,
    )


def get_drive(service: DriveService, *, name_or_id: str) -> SyncDrive:
    return SyncDrive(
        payload=iter_coroutine(service.get_drive(name_or_id=name_or_id)),
        service=service,
    )


def get_or_create_drive(
    service: DriveService,
    *,
    name: str,
    max_size_bytes: int | None = None,
    region: RegionInput = None,
) -> tuple[SyncDrive, bool]:
    state, created = iter_coroutine(
        service.get_or_create_drive(
            name=name,
            max_size_bytes=max_size_bytes,
            region=resolve_region(region, fallback=service.region),
        )
    )
    return SyncDrive(payload=state, service=service), created


def delete_drive(service: DriveService, *, name: str) -> SyncDrive:
    return SyncDrive(
        payload=iter_coroutine(service.delete_drive(name=name)),
        service=service,
    )


def query_drives_page(
    service: DriveService,
    *,
    query: DriveQuery | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
) -> QueryDrivesPage[SyncDrive]:
    page = iter_coroutine(
        service.query_drives_page(query=query, page_size=page_size, cursor=cursor)
    )
    return QueryDrivesPage(
        drives=[SyncDrive(payload=state, service=service) for state in page.drives],
        next_cursor=page.next_cursor,
    )


def query_drives(
    service: DriveService,
    *,
    query: DriveQuery | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
) -> Iterator[SyncDrive]:
    params = QueryDrivesParams(page_size=page_size, cursor=cursor)
    while True:
        page = query_drives_page(
            service,
            query=query,
            page_size=params.page_size,
            cursor=params.cursor,
        )
        yield from page.drives
        if page.next_cursor is None or not page.drives:
            return
        params = params.with_cursor(page.next_cursor)


__all__ = [
    "SyncDrive",
    "delete_drive",
    "fork_drive",
    "get_drive",
    "get_or_create_drive",
    "query_drives",
    "query_drives_page",
]
