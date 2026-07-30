"""Connect service and per-call options."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol, TypeAlias

from vercel._internal.core.http import DEFAULT_API_BASE_URL, DEFAULT_TIMEOUT
from vercel._internal.core.options import ServiceOptions
from vercel.connect._internal.errors import ConnectCredentialsError, ConnectValidationError
from vercel.connect._internal.models import DurationInput

DEFAULT_CONNECT_API_BASE_URL = DEFAULT_API_BASE_URL
DEFAULT_VERCEL_OIDC_ISSUER = "https://oidc.vercel.com"
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


class ConnectCredentialsFactory(Protocol):
    """Resolves the deployment's Vercel OIDC token."""

    async def __call__(self) -> str: ...


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


@dataclass(frozen=True, slots=True, init=False)
class ConnectServiceOptions(ServiceOptions):
    """Configuration for `vercel.connect` calls in an SDK session.

    A session that does not receive this option still constructs one with the
    default Connect API base URL and credential resolver. Supplying the option
    overrides the whole service configuration for that session scope. This is
    both the user configuration seam and the test seam.
    """

    base_url: str
    credentials_factory: ConnectCredentialsFactory | None
    timeout: timedelta
    validity_buffer: timedelta
    token_cache_size: int
    oidc_issuer: str

    def __init__(
        self,
        *,
        base_url: str | None = None,
        credentials_factory: ConnectCredentialsFactory | None = None,
        timeout: timedelta | None = None,
        validity_buffer: timedelta | None = None,
        token_cache_size: int | None = None,
        oidc_issuer: str | None = None,
    ) -> None:
        object.__setattr__(self, "base_url", base_url or DEFAULT_CONNECT_API_BASE_URL)
        # Left as None when unset so the service can pick the resolver that
        # matches the session mode.
        object.__setattr__(self, "credentials_factory", credentials_factory)
        if timeout is not None and timeout.total_seconds() <= 0:
            raise ConnectValidationError("timeout must be positive")
        object.__setattr__(self, "timeout", timeout if timeout is not None else DEFAULT_TIMEOUT)
        # A negative buffer would treat an already-expired token as still usable.
        if validity_buffer is not None and validity_buffer.total_seconds() < 0:
            raise ConnectValidationError("validity_buffer must not be negative")
        object.__setattr__(
            self,
            "validity_buffer",
            validity_buffer if validity_buffer is not None else DEFAULT_VALIDITY_BUFFER,
        )
        # Below one, eviction would pop from an empty mapping.
        if token_cache_size is not None and token_cache_size < 1:
            raise ConnectValidationError("token_cache_size must be at least 1")
        object.__setattr__(
            self,
            "token_cache_size",
            token_cache_size if token_cache_size is not None else DEFAULT_TOKEN_CACHE_SIZE,
        )
        object.__setattr__(self, "oidc_issuer", oidc_issuer or DEFAULT_VERCEL_OIDC_ISSUER)


@dataclass(frozen=True, slots=True)
class ConnectOptions:
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


__all__ = [
    "DEFAULT_CONNECT_API_BASE_URL",
    "DEFAULT_TOKEN_CACHE_SIZE",
    "DEFAULT_VALIDITY_BUFFER",
    "ConnectCredentialsFactory",
    "ConnectOptions",
    "ConnectServiceOptions",
    "VercelTokenInput",
]
