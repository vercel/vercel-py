"""Sandbox service options."""

import inspect
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Protocol, cast

from vercel._internal.auth import TokenProvider
from vercel._internal.unstable.options import ServiceOptions
from vercel._internal.unstable.sandbox.errors import SandboxCredentialsError

DEFAULT_SANDBOX_API_BASE_URL = "https://vercel.com/api"


@dataclass(frozen=True, slots=True)
class SandboxCredentials:
    token: str
    team_id: str
    project_id: str


class SandboxCredentialsFactory(Protocol):
    async def __call__(self) -> SandboxCredentials: ...


def _static_token_provider(token: str) -> TokenProvider:
    async def _provider() -> str:
        return token

    return _provider


def _static_sandbox_credentials_factory(
    *,
    token: str,
    team_id: str,
    project_id: str,
) -> SandboxCredentialsFactory:
    async def _factory() -> SandboxCredentials:
        return SandboxCredentials(token=token, team_id=team_id, project_id=project_id)

    return _factory


async def _resolve_token(token: TokenProvider | None) -> str | None:
    if token is None:
        return None

    result = token()
    if inspect.isawaitable(result):
        return await cast(Awaitable[str], result)
    raise TypeError("SandboxServiceOptions.token provider must return an awaitable string")


def _make_sandbox_credentials_factory(
    *,
    token: str | TokenProvider | None,
    team_id: str | None,
    project_id: str | None,
) -> SandboxCredentialsFactory:
    if isinstance(token, str) and team_id is not None and project_id is not None:
        return _static_sandbox_credentials_factory(
            token=token,
            team_id=team_id,
            project_id=project_id,
        )

    token_provider = _static_token_provider(token) if isinstance(token, str) else token

    async def _factory() -> SandboxCredentials:
        resolved_token = await _resolve_token(token_provider)
        try:
            from vercel.oidc.credentials import get_credentials

            credentials = get_credentials(
                token=resolved_token,
                project_id=project_id,
                team_id=team_id,
            )
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
    base_url: str
    credentials_factory: SandboxCredentialsFactory

    def __init__(
        self,
        *,
        base_url: str | None = None,
        credentials_factory: SandboxCredentialsFactory | None = None,
        token: str | TokenProvider | None = None,
        team_id: str | None = None,
        project_id: str | None = None,
    ) -> None:
        if credentials_factory is not None and (
            token is not None or team_id is not None or project_id is not None
        ):
            raise TypeError(
                "SandboxServiceOptions accepts either credentials_factory or "
                "token/team_id/project_id"
            )

        object.__setattr__(
            self,
            "base_url",
            base_url or DEFAULT_SANDBOX_API_BASE_URL,
        )
        object.__setattr__(
            self,
            "credentials_factory",
            credentials_factory
            or _make_sandbox_credentials_factory(
                token=token,
                team_id=team_id,
                project_id=project_id,
            ),
        )
