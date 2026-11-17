"""Schedules service options."""

import inspect
import os
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Annotated, TypeAlias

from pydantic import Field

from vercel._internal.core.http import DEFAULT_TIMEOUT
from vercel._internal.core.options import ServiceOptions
from vercel.schedules._internal.base import SchedulesModel
from vercel.schedules._internal.errors import SchedulesCredentialsError

DEFAULT_SCHEDULES_BASE_URL = "https://vercel-schedules.com"
"""The public Schedules service endpoint."""

BASE_URL_ENV = "VERCEL_SCHEDULE_BASE_URL"

# `vercel dev` serves a local Schedules API and advertises it with these.
DEV_API_VERSION_ENV = "VERCEL_SCHEDULE_DEV_API_VERSION"
DEV_TOKEN_ENV = "VERCEL_SCHEDULE_TOKEN"
SUPPORTED_DEV_API_VERSION = "1"

SchedulesCredentialsFactory: TypeAlias = Callable[[], Awaitable[str]]
"""Resolves the bearer token sent to the Schedules service, for async sessions."""

SyncSchedulesCredentialsFactory: TypeAlias = Callable[[], str]
"""Resolves the bearer token sent to the Schedules service, for sync sessions."""


def default_base_url() -> str:
    """The base URL from `VERCEL_SCHEDULE_BASE_URL`, else the public endpoint."""
    return os.environ.get(BASE_URL_ENV) or DEFAULT_SCHEDULES_BASE_URL


def _missing_token_error() -> SchedulesCredentialsError:
    return SchedulesCredentialsError(
        "no Vercel OIDC token available; run `vercel env pull` for local "
        "development, or pass SchedulesServiceOptions(credentials_factory=...)"
    )


async def _default_async_credentials() -> str:
    """Resolve the deployment identity for an async session."""
    try:
        from vercel.oidc.aio import get_vercel_oidc_token

        token = await get_vercel_oidc_token()
    except Exception as exc:
        raise SchedulesCredentialsError(str(exc)) from exc

    if not token:
        raise _missing_token_error()
    return token


def _default_sync_credentials() -> str:
    """Resolve the deployment identity for a sync session."""
    try:
        from vercel.oidc import get_vercel_oidc_token

        token = get_vercel_oidc_token()
    except Exception as exc:
        raise SchedulesCredentialsError(str(exc)) from exc

    if not token:
        raise _missing_token_error()
    return token


def local_credentials(base_url: str) -> SyncSchedulesCredentialsFactory | None:
    """The token for the local Schedules API `vercel dev` serves, if `base_url` is it.

    `vercel dev` sets `VERCEL_SCHEDULE_DEV_API_VERSION` and points
    `VERCEL_SCHEDULE_BASE_URL` at its local API, which accepts
    `VERCEL_SCHEDULE_TOKEN` instead of an OIDC token. A deployment, or a base URL
    other than the advertised one, uses the OIDC token as usual.
    """
    version = os.environ.get(DEV_API_VERSION_ENV)
    advertised = os.environ.get(BASE_URL_ENV)
    if version is None or advertised is None or os.environ.get("VERCEL_DEPLOYMENT_ID"):
        return None
    if base_url.rstrip("/") != advertised.rstrip("/"):
        return None

    def resolve() -> str:
        if version != SUPPORTED_DEV_API_VERSION:
            raise SchedulesCredentialsError(
                f"unsupported {DEV_API_VERSION_ENV} {version!r}: this SDK supports local "
                f"Schedules API version {SUPPORTED_DEV_API_VERSION!r}"
            )
        token = os.environ.get(DEV_TOKEN_ENV, "").strip()
        if not token:
            raise SchedulesCredentialsError(
                f"local Schedules API requires {DEV_TOKEN_ENV}, or pass "
                "SchedulesServiceOptions(credentials_factory=...)"
            )
        return token

    return resolve


def static_credentials_factory(token: str) -> SchedulesCredentialsFactory:
    """A credentials factory that always returns `token`, for async sessions."""

    async def factory() -> str:
        return token

    return factory


def sync_static_credentials_factory(token: str) -> SyncSchedulesCredentialsFactory:
    """A credentials factory that always returns `token`, for sync sessions."""

    def factory() -> str:
        return token

    return factory


def adapt_sync_credentials_factory(
    factory: SyncSchedulesCredentialsFactory,
) -> SchedulesCredentialsFactory:
    """Run a sync factory inside the shared async service."""

    async def resolve() -> str:
        token = factory()
        if inspect.isawaitable(token):
            close = getattr(token, "close", None)
            if callable(close):
                close()
            raise SchedulesCredentialsError(
                "a sync session's credentials_factory must return the token, not an "
                "awaitable; use vercel.schedules for an async factory"
            )
        return token

    return resolve


class _SchedulesServiceOptionsKey(ServiceOptions):
    """Shared registry key for sync and async Schedules service options."""

    __slots__ = ()

    @classmethod
    def service_options_key(cls) -> type[ServiceOptions]:
        return _SchedulesServiceOptionsKey


class _SchedulesServiceOptionsBase(SchedulesModel):
    base_url: str = Field(default_factory=default_base_url)
    timeout: Annotated[timedelta, Field(gt=timedelta(0))] = DEFAULT_TIMEOUT


class SchedulesServiceOptions(_SchedulesServiceOptionsBase, _SchedulesServiceOptionsKey):
    """Configuration for `vercel.schedules` calls in an SDK session.

    A session that does not receive this option still constructs one with the
    default base URL and credential resolver. Supplying the option overrides the
    whole service configuration for that session scope.

    Attributes:
        base_url: Schedules service endpoint. Defaults to
            `VERCEL_SCHEDULE_BASE_URL`, else the public endpoint.
        credentials_factory: An async callable resolving the bearer token for
            each request, such as `static_credentials_factory(token)`. Defaults
            to the deployment's OIDC token, or `VERCEL_SCHEDULE_TOKEN` under
            `vercel dev`.
        timeout: HTTP timeout per request.
    """

    credentials_factory: SchedulesCredentialsFactory | None = None

    def resolve_credentials_factory(self) -> SchedulesCredentialsFactory:
        """The factory this session uses."""
        if self.credentials_factory is not None:
            return self.credentials_factory
        local = local_credentials(self.base_url)
        if local is None:
            return _default_async_credentials

        async def resolve_local() -> str:
            return local()

        return resolve_local


class SyncSchedulesServiceOptions(_SchedulesServiceOptionsBase, _SchedulesServiceOptionsKey):
    """Configuration for `vercel.schedules.sync` calls in an SDK session.

    Attributes:
        base_url: Schedules service endpoint. Defaults to
            `VERCEL_SCHEDULE_BASE_URL`, else the public endpoint.
        credentials_factory: A callable resolving the bearer token for each
            request, such as `static_credentials_factory(token)`. It may block.
            Defaults to the deployment's OIDC token, or `VERCEL_SCHEDULE_TOKEN`
            under `vercel dev`.
        timeout: HTTP timeout per request.
    """

    credentials_factory: SyncSchedulesCredentialsFactory | None = None

    def resolve_credentials_factory(self) -> SyncSchedulesCredentialsFactory:
        """The factory this session uses."""
        if self.credentials_factory is not None:
            return self.credentials_factory
        return local_credentials(self.base_url) or _default_sync_credentials


__all__ = [
    "BASE_URL_ENV",
    "DEFAULT_SCHEDULES_BASE_URL",
    "DEV_API_VERSION_ENV",
    "DEV_TOKEN_ENV",
    "SchedulesCredentialsFactory",
    "SchedulesServiceOptions",
    "SyncSchedulesCredentialsFactory",
    "SyncSchedulesServiceOptions",
    "adapt_sync_credentials_factory",
    "default_base_url",
    "local_credentials",
    "static_credentials_factory",
    "sync_static_credentials_factory",
]
