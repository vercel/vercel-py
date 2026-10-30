"""Sandbox service options."""

from collections.abc import Callable, Coroutine
from datetime import timedelta
from typing import Any, Protocol

from vercel._internal.core.options import ServiceOptions
from vercel.sandbox._internal.errors import SandboxCredentialsError
from vercel.sandbox._internal.region import RegionInput
from vercel.sandbox._internal.value_model import SandboxValueModel

DEFAULT_SANDBOX_API_BASE_URL = "https://vercel.com/api"
_DEFAULT_FILE_TRANSFER_TIMEOUT = timedelta(minutes=5)


class SandboxCredentials(SandboxValueModel):
    token: str
    team_id: str
    project_id: str

    def __init__(self, token: str, team_id: str, project_id: str) -> None:
        super().__init__(token=token, team_id=team_id, project_id=project_id)


class SandboxCredentialsFactory(Protocol):
    async def __call__(self) -> SandboxCredentials: ...


class SyncSandboxCredentialsFactory(Protocol):
    def __call__(self) -> SandboxCredentials: ...


def _resolve_default_sandbox_credentials() -> SandboxCredentials:
    try:
        from vercel.oidc.credentials import get_credentials

        credentials = get_credentials()
    except Exception as exc:
        raise SandboxCredentialsError(str(exc)) from exc

    return SandboxCredentials(
        token=credentials.token,
        team_id=credentials.team_id,
        project_id=credentials.project_id,
    )


async def _default_sandbox_credentials_factory() -> SandboxCredentials:
    return _resolve_default_sandbox_credentials()


def _explicit_options(**options: Any) -> dict[str, Any]:
    """Drop unset (None or empty-string) values so field defaults apply."""
    return {key: value for key, value in options.items() if value is not None and value != ""}


class _SandboxServiceOptionsKey(ServiceOptions):
    """Shared registry key for sync and async Sandbox service options."""

    __slots__ = ()

    @classmethod
    def service_options_key(cls) -> type[ServiceOptions]:
        return _SandboxServiceOptionsKey


class SandboxServiceOptions(SandboxValueModel, _SandboxServiceOptionsKey):
    """Configuration for `vercel.sandbox` calls in an SDK session.

    A session that does not receive this option still constructs one with the
    default Sandbox API base URL and credential resolver. Supplying the option
    overrides the whole service configuration for that session scope.

    ``region`` defaults to ``None``, leaving placement to the API. Pass
    ``CURRENT_REGION`` to use ``VERCEL_REGION`` at request time when available.
    """

    base_url: str = DEFAULT_SANDBOX_API_BASE_URL
    credentials_factory: Callable[[], Coroutine[Any, Any, SandboxCredentials]] = (
        _default_sandbox_credentials_factory
    )
    file_transfer_timeout: timedelta = _DEFAULT_FILE_TRANSFER_TIMEOUT
    region: RegionInput = None

    def __init__(
        self,
        *,
        base_url: str | None = None,
        credentials_factory: SandboxCredentialsFactory | None = None,
        file_transfer_timeout: timedelta | None = None,
        region: RegionInput = None,
    ) -> None:
        super().__init__(
            **_explicit_options(
                base_url=base_url,
                credentials_factory=credentials_factory,
                file_transfer_timeout=file_transfer_timeout,
            ),
            region=region,
        )


class SyncSandboxServiceOptions(SandboxValueModel, _SandboxServiceOptionsKey):
    """Configuration for synchronous `vercel.sandbox` calls in an SDK session.

    ``region`` defaults to ``None``. Pass ``CURRENT_REGION`` to use the current
    function's ``VERCEL_REGION`` at request time when available.
    """

    base_url: str = DEFAULT_SANDBOX_API_BASE_URL
    credentials_factory: Callable[[], SandboxCredentials] = _resolve_default_sandbox_credentials
    file_transfer_timeout: timedelta = _DEFAULT_FILE_TRANSFER_TIMEOUT
    region: RegionInput = None

    def __init__(
        self,
        *,
        base_url: str | None = None,
        credentials_factory: SyncSandboxCredentialsFactory | None = None,
        file_transfer_timeout: timedelta | None = None,
        region: RegionInput = None,
    ) -> None:
        super().__init__(
            **_explicit_options(
                base_url=base_url,
                credentials_factory=credentials_factory,
                file_transfer_timeout=file_transfer_timeout,
            ),
            region=region,
        )
