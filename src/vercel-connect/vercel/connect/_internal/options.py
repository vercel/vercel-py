"""Connect service and per-call options."""

from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Annotated, TypeAlias

from pydantic import Field, field_validator

from vercel._internal.core.http import DEFAULT_API_BASE_URL, DEFAULT_TIMEOUT
from vercel._internal.core.options import ServiceOptions
from vercel.connect._internal.base import ConnectModel, reject_bool
from vercel.connect._internal.errors import ConnectCredentialsError
from vercel.connect._internal.models import DurationInput

DEFAULT_CONNECT_API_BASE_URL = DEFAULT_API_BASE_URL
DEFAULT_VALIDITY_BUFFER = timedelta(seconds=30)
DEFAULT_TOKEN_CACHE_SIZE = 100

VercelTokenInput: TypeAlias = str | Callable[[], str | Awaitable[str]]
"""A platform identity token, or a callable resolving one at call time.

Prefer the callable form for anything long-lived: OIDC tokens are short-lived,
so a captured string goes stale.

A callable returning an awaitable is only usable from the async surface. The sync
surface steps its coroutine exactly once, so a callable that actually suspends —
one performing network I/O, for instance — cannot be resolved there. Supply a
plain synchronous callable when calling `vercel.connect.sync`.
"""


ConnectCredentialsFactory: TypeAlias = Callable[[], Awaitable[str]]
"""Resolves the deployment's Vercel OIDC token.

A callable rather than a `Protocol`, so it can be a validated field of
`ConnectServiceOptions`: pydantic cannot build a schema for a protocol.
"""


def _missing_token_error() -> ConnectCredentialsError:
    return ConnectCredentialsError(
        "no Vercel OIDC token available; run `vercel env pull` for local "
        "development, or pass options=ConnectOptions(vercel_token=...)"
    )


async def _default_async_credentials_factory() -> str:
    """Resolve the deployment identity for an async session."""
    try:
        from vercel.oidc.aio import get_vercel_oidc_token

        token = await get_vercel_oidc_token()
    except Exception as exc:
        raise ConnectCredentialsError(str(exc)) from exc

    if not token:
        raise _missing_token_error()
    return token


async def _default_sync_credentials_factory() -> str:
    """Resolve the deployment identity for a sync session.

    Declared async to satisfy the shared service, but deliberately calls the
    purely synchronous resolver: the sync surface is driven by `iter_coroutine`,
    which cannot tolerate a suspension. The async resolver awaits an HTTP refresh
    on the local-dev path, so using it here would fail exactly when a token needs
    refreshing. `vercel-sandbox` makes the same choice for the same reason.
    """
    try:
        from vercel.oidc import get_vercel_oidc_token

        token = get_vercel_oidc_token()
    except Exception as exc:
        raise ConnectCredentialsError(str(exc)) from exc

    if not token:
        raise _missing_token_error()
    return token


class ConnectServiceOptions(ServiceOptions, ConnectModel):
    """Configuration for `vercel.connect` calls in an SDK session.

    A session that does not receive this option still constructs one with the
    default Connect API base URL and credential resolver. Supplying the option
    overrides the whole service configuration for that session scope. This is
    both the user configuration seam and the test seam.
    """

    base_url: str = DEFAULT_CONNECT_API_BASE_URL
    # `None` until the service picks the resolver matching the session mode.
    credentials_factory: ConnectCredentialsFactory | None = None
    timeout: Annotated[timedelta, Field(gt=timedelta(0))] = DEFAULT_TIMEOUT
    # A negative buffer would treat an already-expired token as still usable.
    validity_buffer: Annotated[timedelta, Field(ge=timedelta(0))] = DEFAULT_VALIDITY_BUFFER
    # Below one, eviction would pop from an empty mapping.
    token_cache_size: Annotated[int, Field(ge=1)] = DEFAULT_TOKEN_CACHE_SIZE


class ConnectOptions(ConnectModel):
    """Per-call overrides for a single Connect operation.

    Attributes:
        vercel_token: Supply the platform identity yourself instead of resolving
            it from the environment. Accepts a string or a callable resolved at
            call time.
        force_refresh: Bypass the local token cache and re-validate upstream.
            Use this to surface a grant revoked elsewhere, and to poll a
            device-code authorization to completion.
        no_cache: Neither read from nor write to the local token cache.
        validity_buffer: Treat a cached token as stale this long before its
            expiry. Client-side read policy only; never sent to the server.
    """

    vercel_token: VercelTokenInput | None = None
    force_refresh: bool = False
    no_cache: bool = False
    validity_buffer: DurationInput | None = None

    @field_validator("validity_buffer", mode="before")
    @classmethod
    def _check_validity_buffer(cls, value: object) -> object:
        return reject_bool(value)


__all__ = [
    "DEFAULT_CONNECT_API_BASE_URL",
    "DEFAULT_TOKEN_CACHE_SIZE",
    "DEFAULT_VALIDITY_BUFFER",
    "ConnectCredentialsFactory",
    "ConnectOptions",
    "ConnectServiceOptions",
    "VercelTokenInput",
]
