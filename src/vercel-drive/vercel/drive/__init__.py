"""Vercel Drives SDK surface.

A Drive is a named persistent filesystem that belongs to one project and one
region. Create or look up Drives here, then mount them into a sandbox with
`vercel.sandbox`.

Requests authenticate with the deployment's Vercel OIDC token, or with
`VERCEL_TOKEN`, `VERCEL_TEAM_ID`, and `VERCEL_PROJECT_ID`. To supply
credentials yourself, pass `DriveServiceOptions(credentials_factory=...)` in a
session.
"""

from collections.abc import AsyncIterator

from vercel._internal.core.region import CURRENT_REGION, CurrentRegion, RegionInput
from vercel._internal.core.session import get_active_session
from vercel.drive._internal.async_runtime import (
    Drive,
    delete_drive as _delete_drive,
    fork_drive as _fork_drive,
    get_drive as _get_drive,
    get_or_create_drive as _get_or_create_drive,
    query_drives as _query_drives,
)
from vercel.drive._internal.errors import (
    DriveApiError,
    DriveCredentialsError,
    DriveError,
    DriveResponseError,
)
from vercel.drive._internal.models import (
    MAX_QUERY_DRIVES_PAGE_SIZE,
    DriveHandle,
    DriveMount,
    DriveMountMode,
    DriveQuery,
    DriveQueryByCreatedAt,
    DriveQueryByName,
    DriveQueryByUpdatedAt,
    DriveReference,
)
from vercel.drive._internal.options import (
    DEFAULT_DRIVE_API_BASE_URL,
    DriveCredentials,
    DriveCredentialsFactory,
    DriveServiceOptions,
)
from vercel.drive._internal.service import DriveService, get_drive_service
from vercel.drive.version import __version__

from . import sync


def _service() -> DriveService:
    return get_drive_service(get_active_session())


async def get_drive(*, name_or_id: str) -> Drive:
    """Look up a Drive by name or ID without creating it.

    Raises:
        DriveApiError: With status code 404 if the Drive does not exist.
    """
    return await _get_drive(_service(), name_or_id=name_or_id)


async def get_or_create_drive(
    *,
    name: str,
    max_size_bytes: int | None = None,
    region: RegionInput = None,
) -> tuple[Drive, bool]:
    """Return a named Drive, creating it when necessary.

    Args:
        name: Project-local Drive name.
        max_size_bytes: Maximum size for a new Drive. Uses the project's default
            when omitted.
        region: Storage region for a new Drive. Uses `DriveServiceOptions.region`
            when omitted. Pass `CURRENT_REGION` to use the current function's
            region if available. An existing Drive must use the requested region.

    Returns:
        A `(drive, created)` tuple. `created` is true when this call created
        the Drive, and false when it returned an existing Drive.
    """
    return await _get_or_create_drive(
        _service(),
        name=name,
        max_size_bytes=max_size_bytes,
        region=region,
    )


async def fork_drive(*, source: str, name: str) -> Drive:
    """Fork a Drive by project-local name without fetching it first.

    The new Drive inherits the source's committed data, region, and maximum
    size.

    Args:
        source: Project-local source Drive name.
        name: Project-local name for the new Drive.

    Raises:
        DriveApiError: With status code 404 if the source does not exist, or
            409 if the name is taken or the source is uninitialized.
    """
    return await _fork_drive(_service(), source=source, name=name)


async def delete_drive(*, name: str) -> Drive:
    """Delete a Drive by project-local name.

    Use this when no Drive handle is available, such as cleanup after a partial
    creation failure. Otherwise, prefer `await drive.delete()`. A Drive must
    be unmounted before it can be deleted.

    Args:
        name: Project-local Drive name.

    Returns:
        The Drive returned by the deletion request.
    """
    return await _delete_drive(_service(), name=name)


def query_drives(
    *,
    query: DriveQuery | None = None,
    page_size: int | None = None,
    cursor: str | None = None,
) -> AsyncIterator[Drive]:
    """Iterate over Drives in a project.

    Args:
        query: Ordering and optional name-prefix filter.
        page_size: Maximum number of Drives fetched per API request.
        cursor: Cursor at which to begin pagination.

    Returns:
        An async iterator that transparently follows pagination cursors.
    """
    return _query_drives(
        _service(),
        query=query,
        page_size=page_size,
        cursor=cursor,
    )


__all__ = [
    "CURRENT_REGION",
    "DEFAULT_DRIVE_API_BASE_URL",
    "MAX_QUERY_DRIVES_PAGE_SIZE",
    "CurrentRegion",
    "Drive",
    "DriveApiError",
    "DriveCredentials",
    "DriveCredentialsError",
    "DriveCredentialsFactory",
    "DriveError",
    "DriveHandle",
    "DriveMount",
    "DriveMountMode",
    "DriveQuery",
    "DriveQueryByCreatedAt",
    "DriveQueryByName",
    "DriveQueryByUpdatedAt",
    "DriveReference",
    "DriveResponseError",
    "DriveServiceOptions",
    "RegionInput",
    "__version__",
    "delete_drive",
    "fork_drive",
    "get_drive",
    "get_or_create_drive",
    "query_drives",
    "sync",
]
