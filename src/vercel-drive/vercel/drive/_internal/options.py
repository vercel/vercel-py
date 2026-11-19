"""Drive service options."""

from collections.abc import Callable, Coroutine
from datetime import timedelta
from typing import Any, Protocol

from vercel._internal.core.http import DEFAULT_API_BASE_URL
from vercel._internal.core.options import ServiceOptions
from vercel._internal.core.region import RegionInput
from vercel.drive._internal.errors import DriveCredentialsError
from vercel.drive._internal.models import DriveValueModel

DEFAULT_DRIVE_API_BASE_URL = DEFAULT_API_BASE_URL
"""The Vercel API endpoint that serves Drives."""


class DriveCredentials(DriveValueModel):
    """The token, team, and project a Drive request is made for."""

    token: str
    team_id: str
    project_id: str

    def __init__(self, token: str, team_id: str, project_id: str) -> None:
        super().__init__(token=token, team_id=team_id, project_id=project_id)


class DriveCredentialsFactory(Protocol):
    """Resolves the credentials for each request, for async sessions."""

    async def __call__(self) -> DriveCredentials: ...


class SyncDriveCredentialsFactory(Protocol):
    """Resolves the credentials for each request, for sync sessions."""

    def __call__(self) -> DriveCredentials: ...


def _resolve_default_drive_credentials() -> DriveCredentials:
    try:
        from vercel.oidc.credentials import get_credentials

        credentials = get_credentials()
    except Exception as exc:
        raise DriveCredentialsError(str(exc)) from exc

    return DriveCredentials(
        token=credentials.token,
        team_id=credentials.team_id,
        project_id=credentials.project_id,
    )


async def _default_drive_credentials_factory() -> DriveCredentials:
    return _resolve_default_drive_credentials()


def _explicit_options(**options: Any) -> dict[str, Any]:
    """Drop unset (None or empty-string) values so field defaults apply."""
    return {key: value for key, value in options.items() if value is not None and value != ""}


class _DriveServiceOptionsKey(ServiceOptions):
    """Shared registry key for sync and async Drive service options."""

    __slots__ = ()

    @classmethod
    def service_options_key(cls) -> type[ServiceOptions]:
        return _DriveServiceOptionsKey


class DriveServiceOptions(DriveValueModel, _DriveServiceOptionsKey):
    """Configuration for `vercel.drive` calls in an SDK session.

    A session that does not receive this option still constructs one with the
    default API base URL and credential resolver. Supplying the option
    overrides the whole service configuration for that session scope.

    Requests retry network failures, HTTP 429, and HTTP 5xx twice.

    Args:
        base_url: Vercel API endpoint.
        credentials_factory: An async callable resolving the token, team, and
            project for each request. Defaults to the Vercel OIDC token, or
            `VERCEL_TOKEN`, `VERCEL_TEAM_ID`, and `VERCEL_PROJECT_ID`.
        request_timeout: HTTP I/O limit per attempt. Defaults to no read
            timeout, retaining the HTTP client's other limits.
        region: Storage region for Drives this session creates. Defaults to
            `None`, leaving placement to the API. Pass `CURRENT_REGION` to use
            `VERCEL_REGION` when a Drive is created.
    """

    base_url: str = DEFAULT_DRIVE_API_BASE_URL
    credentials_factory: Callable[[], Coroutine[Any, Any, DriveCredentials]] = (
        _default_drive_credentials_factory
    )
    request_timeout: timedelta | None = None
    region: RegionInput = None

    def __init__(
        self,
        *,
        base_url: str | None = None,
        credentials_factory: DriveCredentialsFactory | None = None,
        request_timeout: timedelta | None = None,
        region: RegionInput = None,
    ) -> None:
        super().__init__(
            **_explicit_options(
                base_url=base_url,
                credentials_factory=credentials_factory,
                request_timeout=request_timeout,
            ),
            region=region,
        )


class SyncDriveServiceOptions(DriveValueModel, _DriveServiceOptionsKey):
    """Configuration for `vercel.drive.sync` calls in an SDK session.

    Fields have the same meaning as in `DriveServiceOptions`, except that
    `credentials_factory` is a plain callable, which may block.
    """

    base_url: str = DEFAULT_DRIVE_API_BASE_URL
    credentials_factory: Callable[[], DriveCredentials] = _resolve_default_drive_credentials
    request_timeout: timedelta | None = None
    region: RegionInput = None

    def __init__(
        self,
        *,
        base_url: str | None = None,
        credentials_factory: SyncDriveCredentialsFactory | None = None,
        request_timeout: timedelta | None = None,
        region: RegionInput = None,
    ) -> None:
        super().__init__(
            **_explicit_options(
                base_url=base_url,
                credentials_factory=credentials_factory,
                request_timeout=request_timeout,
            ),
            region=region,
        )


__all__ = [
    "DEFAULT_DRIVE_API_BASE_URL",
    "DriveCredentials",
    "DriveCredentialsFactory",
    "DriveServiceOptions",
    "SyncDriveCredentialsFactory",
    "SyncDriveServiceOptions",
]
