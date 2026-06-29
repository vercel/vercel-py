"""Sandbox service options."""

from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol

from vercel._internal.unstable.options import ServiceOptions
from vercel._internal.unstable.sandbox.errors import SandboxCredentialsError

DEFAULT_SANDBOX_API_BASE_URL = "https://vercel.com/api"
_DEFAULT_FILE_TRANSFER_TIMEOUT = timedelta(minutes=5)


@dataclass(frozen=True, slots=True)
class SandboxCredentials:
    token: str
    team_id: str
    project_id: str


class SandboxCredentialsFactory(Protocol):
    async def __call__(self) -> SandboxCredentials: ...


def _default_sandbox_credentials_factory() -> SandboxCredentialsFactory:
    async def _factory() -> SandboxCredentials:
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

    return _factory


@dataclass(frozen=True, slots=True, init=False)
class SandboxServiceOptions(ServiceOptions):
    """Configuration for `vercel.unstable.sandbox` calls in an SDK session.

    A session that does not receive this option still constructs one with the
    default Sandbox API base URL and credential resolver. Supplying the option
    overrides the whole service configuration for that session scope.
    """

    base_url: str
    credentials_factory: SandboxCredentialsFactory
    file_transfer_timeout: timedelta

    def __init__(
        self,
        *,
        base_url: str | None = None,
        credentials_factory: SandboxCredentialsFactory | None = None,
        file_transfer_timeout: timedelta | None = None,
    ) -> None:
        object.__setattr__(
            self,
            "base_url",
            base_url or DEFAULT_SANDBOX_API_BASE_URL,
        )
        object.__setattr__(
            self,
            "credentials_factory",
            credentials_factory or _default_sandbox_credentials_factory(),
        )
        object.__setattr__(
            self,
            "file_transfer_timeout",
            file_transfer_timeout
            if file_transfer_timeout is not None
            else _DEFAULT_FILE_TRANSFER_TIMEOUT,
        )
