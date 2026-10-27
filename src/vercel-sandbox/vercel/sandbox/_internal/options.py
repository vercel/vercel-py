"""Sandbox service options."""

import os
from collections.abc import Callable, Coroutine
from datetime import timedelta
from typing import Any, Protocol

from pydantic import Field

from vercel._internal.core.options import ServiceOptions
from vercel.sandbox._internal.errors import SandboxCredentialsError
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
    """

    base_url: str = DEFAULT_SANDBOX_API_BASE_URL
    credentials_factory: Callable[[], Coroutine[Any, Any, SandboxCredentials]] = (
        _default_sandbox_credentials_factory
    )
    file_transfer_timeout: timedelta = _DEFAULT_FILE_TRANSFER_TIMEOUT
    region: str | None = Field(default_factory=lambda: os.environ.get("VERCEL_REGION") or None)

    def __init__(
        self,
        *,
        base_url: str | None = None,
        credentials_factory: SandboxCredentialsFactory | None = None,
        file_transfer_timeout: timedelta | None = None,
        region: str | None = None,
    ) -> None:
        super().__init__(
            base_url=base_url or DEFAULT_SANDBOX_API_BASE_URL,
            credentials_factory=credentials_factory or _default_sandbox_credentials_factory,
            file_transfer_timeout=(
                file_transfer_timeout
                if file_transfer_timeout is not None
                else _DEFAULT_FILE_TRANSFER_TIMEOUT
            ),
            region=region or os.environ.get("VERCEL_REGION") or None,
        )


class SyncSandboxServiceOptions(SandboxValueModel, _SandboxServiceOptionsKey):
    """Configuration for synchronous `vercel.sandbox` calls in an SDK session."""

    base_url: str = DEFAULT_SANDBOX_API_BASE_URL
    credentials_factory: Callable[[], SandboxCredentials] = _resolve_default_sandbox_credentials
    file_transfer_timeout: timedelta = _DEFAULT_FILE_TRANSFER_TIMEOUT
    region: str | None = Field(default_factory=lambda: os.environ.get("VERCEL_REGION") or None)

    def __init__(
        self,
        *,
        base_url: str | None = None,
        credentials_factory: SyncSandboxCredentialsFactory | None = None,
        file_transfer_timeout: timedelta | None = None,
        region: str | None = None,
    ) -> None:
        super().__init__(
            base_url=base_url or DEFAULT_SANDBOX_API_BASE_URL,
            credentials_factory=credentials_factory or _resolve_default_sandbox_credentials,
            file_transfer_timeout=(
                file_transfer_timeout
                if file_transfer_timeout is not None
                else _DEFAULT_FILE_TRANSFER_TIMEOUT
            ),
            region=region or os.environ.get("VERCEL_REGION") or None,
        )
