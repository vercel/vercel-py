"""Neutral orchestration for Drive operations.

All behavior lives here, async-only and mode-agnostic, so the sync and async
runtimes share one implementation.
"""

from collections.abc import Callable
from typing import TYPE_CHECKING

from vercel._internal.core.region import RegionInput, ResolvedRegion
from vercel.drives._internal.api_client import DriveApiClient
from vercel.drives._internal.models import DriveQuery, DrivesPageState, DriveState
from vercel.drives._internal.options import (
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
        return DriveService(
            api_client=DriveApiClient(
                base_url=options.base_url,
                credentials_factory=options.credentials_factory,
                transport=session.get_transport(),
                request_timeout=options.request_timeout,
                sleep=session.sleep,
            ),
            ensure_open=session.check_open,
            region=options.region,
        )

    return session.get_or_create_service(DriveService, factory)


def get_sync_drive_service(session: "SyncSdkSession") -> DriveService:
    def factory() -> DriveService:
        options = session.get_service_option(SyncDriveServiceOptions) or SyncDriveServiceOptions()
        return DriveService(
            api_client=DriveApiClient(
                base_url=options.base_url,
                credentials_factory=adapt_sync_credentials_factory(options.credentials_factory),
                transport=session.get_transport(),
                request_timeout=options.request_timeout,
                sleep=session.sleep,
            ),
            ensure_open=session.check_open,
            region=options.region,
        )

    return session.get_or_create_service(DriveService, factory)


__all__ = [
    "DriveService",
    "adapt_sync_credentials_factory",
    "get_drive_service",
    "get_sync_drive_service",
]
