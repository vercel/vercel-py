"""Connect service and per-call options."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol, TypeAlias

from vercel._internal.core.http import DEFAULT_API_BASE_URL, DEFAULT_TIMEOUT
from vercel._internal.core.options import ServiceOptions
from vercel.connect._internal.errors import ConnectCredentialsError
from vercel.connect._internal.models import DurationInput

DEFAULT_CONNECT_API_BASE_URL = DEFAULT_API_BASE_URL
DEFAULT_VERCEL_OIDC_ISSUER = "https://oidc.vercel.com"
DEFAULT_VALIDITY_BUFFER = timedelta(seconds=30)
DEFAULT_TOKEN_CACHE_SIZE = 100

VercelTokenInput: TypeAlias = str | Callable[[], str | Awaitable[str]]
"""A platform identity token, or a callable resolving one at call time.

Prefer the callable form for anything long-lived: OIDC tokens are short-lived,
so a captured string goes stale.
"""


class ConnectCredentialsFactory(Protocol):
    """Resolves the deployment's Vercel OIDC token."""

    async def __call__(self) -> str: ...


async def _default_connect_credentials_factory() -> str:
    try:
        from vercel.oidc.aio import get_vercel_oidc_token

        token = await get_vercel_oidc_token()
    except Exception as exc:
        raise ConnectCredentialsError(str(exc)) from exc

    if not token:
        raise ConnectCredentialsError(
            "no Vercel OIDC token available; run `vercel env pull` for local "
            "development, or pass options=ConnectOptions(vercel_token=...)"
        )
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
    credentials_factory: ConnectCredentialsFactory
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
        object.__setattr__(
            self,
            "credentials_factory",
            credentials_factory or _default_connect_credentials_factory,
        )
        object.__setattr__(self, "timeout", timeout if timeout is not None else DEFAULT_TIMEOUT)
        object.__setattr__(
            self,
            "validity_buffer",
            validity_buffer if validity_buffer is not None else DEFAULT_VALIDITY_BUFFER,
        )
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
