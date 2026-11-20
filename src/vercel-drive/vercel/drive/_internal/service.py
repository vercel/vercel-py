"""Neutral orchestration for Drive operations.

All behavior lives here, async-only and mode-agnostic, so the sync and async
runtimes share one implementation.
"""

from collections.abc import Callable
from datetime import timedelta
from typing import TYPE_CHECKING

from vercel._internal.core.region import RegionInput, ResolvedRegion
from vercel.drive._internal.api_client import DriveApiClient
from vercel.drive._internal.errors import DriveApiError, DriveResponseError
from vercel.drive._internal.models import DriveQuery, DrivesPageState, DriveState
from vercel.drive._internal.options import (
    DriveCredentials,
    DriveCredentialsFactory,
    DriveServiceOptions,
    SyncDriveCredentialsFactory,
    SyncDriveServiceOptions,
)

if TYPE_CHECKING:
    from vercel._internal.core.session import SdkSession, SyncSdkSession


def _check_name(name: str, label: str) -> str:
    # URL parsing drops an empty path segment and resolves `.` and `..`, so these
    # names would reach the list route or another resource instead.
    if not isinstance(name, str):
        raise TypeError(f"{label} must be a string")
    if name.strip() in ("", ".", ".."):
        raise ValueError(f"{label} {name!r} is not a valid Drive name")
    return name


class DriveService:
    """Async-only Drive orchestration returning neutral state."""

    def __init__(
        self,
        *,
        api_client: DriveApiClient,
        ensure_open: Callable[[], None],
        region: RegionInput = None,
    ) -> None:
        self._api_client = api_client
        self._ensure_open = ensure_open
        self._region = region

    @property
    def region(self) -> RegionInput:
        """The region used for Drives created without an explicit one."""
        return self._region

    @property
    def response_error(self) -> type[DriveResponseError]:
        """The error raised for a successful response the SDK cannot accept."""
        return self._api_client.response_error

    async def get_drive(self, *, name_or_id: str) -> DriveState:
        self._ensure_open()
        return await self._api_client.get_drive(name_or_id=_check_name(name_or_id, "name_or_id"))

    async def get_or_create_drive(
        self,
        *,
        name: str,
        max_size_bytes: int | None = None,
        region: ResolvedRegion | None = None,
    ) -> tuple[DriveState, bool]:
        self._ensure_open()
        return await self._api_client.get_or_create_drive(
            name=_check_name(name, "name"),
            max_size_bytes=max_size_bytes,
            region=region,
        )

    async def fork_drive(self, *, name: str, fork_name: str) -> DriveState:
        self._ensure_open()
        return await self._api_client.fork_drive(
            name=_check_name(name, "source"), fork_name=_check_name(fork_name, "name")
        )

    async def delete_drive(self, *, name: str) -> DriveState:
        self._ensure_open()
        return await self._api_client.delete_drive(name=_check_name(name, "name"))

    async def query_drives_page(
        self,
        *,
        query: DriveQuery | None = None,
        page_size: int | None = None,
        cursor: str | None = None,
    ) -> DrivesPageState:
        self._ensure_open()
        return await self._api_client.query_drives(query=query, limit=page_size, cursor=cursor)


def adapt_sync_credentials_factory(
    factory: SyncDriveCredentialsFactory,
) -> DriveCredentialsFactory:
    """Run a sync factory inside the shared async service."""

    async def credentials_factory() -> DriveCredentials:
        return factory()

    return credentials_factory


def get_drive_service(session: "SdkSession") -> DriveService:
    def factory() -> DriveService:
        options = session.get_service_option(DriveServiceOptions) or DriveServiceOptions()
        return create_drive_service(
            session,
            base_url=options.base_url,
            credentials_factory=options.credentials_factory,
            request_timeout=options.request_timeout,
            region=options.region,
        )

    return session.get_or_create_service(DriveService, factory)


def get_sync_drive_service(session: "SyncSdkSession") -> DriveService:
    def factory() -> DriveService:
        options = session.get_service_option(SyncDriveServiceOptions) or SyncDriveServiceOptions()
        return create_drive_service(
            session,
            base_url=options.base_url,
            credentials_factory=adapt_sync_credentials_factory(options.credentials_factory),
            request_timeout=options.request_timeout,
            region=options.region,
        )

    return session.get_or_create_service(DriveService, factory)


def create_drive_service(
    session: "SdkSession | SyncSdkSession",
    *,
    base_url: str,
    credentials_factory: DriveCredentialsFactory,
    request_timeout: timedelta | None,
    region: RegionInput,
    api_error: type[DriveApiError] = DriveApiError,
    response_error: type[DriveResponseError] = DriveResponseError,
) -> DriveService:
    """Build a Drive service on a session's transport, without caching it.

    `vercel-sandbox` uses this to run Drive operations with its own service
    options and error types.
    """
    return DriveService(
        api_client=DriveApiClient(
            base_url=base_url,
            credentials_factory=credentials_factory,
            transport=session.get_transport(),
            request_timeout=request_timeout,
            sleep=session.sleep,
            api_error=api_error,
            response_error=response_error,
        ),
        ensure_open=session.check_open,
        region=region,
    )


__all__ = [
    "DriveService",
    "adapt_sync_credentials_factory",
    "create_drive_service",
    "get_drive_service",
    "get_sync_drive_service",
]
