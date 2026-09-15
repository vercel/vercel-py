"""Schedules service options."""

import os
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Annotated, TypeAlias

from pydantic import Field, model_validator

from vercel._internal.core.http import DEFAULT_TIMEOUT
from vercel._internal.core.options import ServiceOptions
from vercel.schedules._internal.base import SchedulesModel
from vercel.schedules._internal.errors import (
    SchedulesCredentialsError,
    SchedulesValidationError,
)

DEFAULT_SCHEDULES_BASE_URL = "https://vss-server.vercel.sh"
"""The public Schedules service endpoint."""

BASE_URL_ENV = "VERCEL_SCHEDULE_BASE_URL"

SchedulesCredentialsFactory: TypeAlias = Callable[[], Awaitable[str]]
"""Resolves the bearer token sent to the Schedules service.

A callable rather than a `Protocol`, so it can be a validated field of
`SchedulesServiceOptions`: pydantic cannot build a schema for a protocol.
"""


def default_base_url() -> str:
    """The base URL from `VERCEL_SCHEDULE_BASE_URL`, else the public endpoint."""
    return os.environ.get(BASE_URL_ENV) or DEFAULT_SCHEDULES_BASE_URL


def _missing_token_error() -> SchedulesCredentialsError:
    return SchedulesCredentialsError(
        "no Vercel OIDC token available; run `vercel env pull` for local "
        "development, or pass SchedulesServiceOptions(token=...)"
    )


async def _default_async_credentials_factory() -> str:
    """Resolve the deployment identity for an async session."""
    try:
        from vercel.oidc.aio import get_vercel_oidc_token

        token = await get_vercel_oidc_token()
    except Exception as exc:
        raise SchedulesCredentialsError(str(exc)) from exc

    if not token:
        raise _missing_token_error()
    return token


async def _default_sync_credentials_factory() -> str:
    """Resolve the deployment identity for a sync session.

    Declared async to satisfy the shared service, but deliberately calls the
    purely synchronous resolver: the sync surface is driven by `iter_coroutine`,
    which cannot tolerate a suspension. The async resolver awaits an HTTP refresh
    on the local-dev path, so using it here would fail exactly when a token needs
    refreshing. `vercel-connect` and `vercel-sandbox` make the same choice.
    """
    try:
        from vercel.oidc import get_vercel_oidc_token

        token = get_vercel_oidc_token()
    except Exception as exc:
        raise SchedulesCredentialsError(str(exc)) from exc

    if not token:
        raise _missing_token_error()
    return token


def static_credentials_factory(token: str) -> SchedulesCredentialsFactory:
    """A factory that always returns one caller-supplied token."""

    async def factory() -> str:
        return token

    return factory


class SchedulesServiceOptions(ServiceOptions, SchedulesModel):
    """Configuration for `vercel.schedules` calls in an SDK session.

    A session that does not receive this option still constructs one with the
    default base URL and credential resolver. Supplying the option overrides the
    whole service configuration for that session scope. This is both the user
    configuration seam and the test seam.

    Attributes:
        base_url: Schedules service endpoint. Defaults to
            `VERCEL_SCHEDULE_BASE_URL`, else the public endpoint.
        token: A fixed bearer token, instead of resolving the deployment's OIDC
            token. Mutually exclusive with `credentials_factory`.
        credentials_factory: A callable resolving the bearer token per request.
        timeout: HTTP timeout per request.
    """

    base_url: str = Field(default_factory=default_base_url)
    token: str | None = None
    credentials_factory: SchedulesCredentialsFactory | None = None
    timeout: Annotated[timedelta, Field(gt=timedelta(0))] = DEFAULT_TIMEOUT

    @model_validator(mode="after")
    def _one_credential_source(self) -> "SchedulesServiceOptions":
        if self.token is not None and self.credentials_factory is not None:
            raise SchedulesValidationError("pass either token or credentials_factory, not both")
        return self

    def resolve_credentials_factory(self, *, sync: bool) -> SchedulesCredentialsFactory:
        """The factory this session should use, given its mode."""
        if self.credentials_factory is not None:
            return self.credentials_factory
        if self.token is not None:
            return static_credentials_factory(self.token)
        return _default_sync_credentials_factory if sync else _default_async_credentials_factory


__all__ = [
    "BASE_URL_ENV",
    "DEFAULT_SCHEDULES_BASE_URL",
    "SchedulesCredentialsFactory",
    "SchedulesServiceOptions",
    "default_base_url",
    "static_credentials_factory",
]
