"""Neutral orchestration for Connect operations.

All business logic lives here, async-only and mode-agnostic, so the sync and
async runtimes share one implementation.
"""

import inspect
import os
import warnings
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlsplit

from vercel._internal.core.time import coerce_duration
from vercel.connect._internal.api_client import ConnectApiClient
from vercel.connect._internal.cache import TokenCache, build_cache_key
from vercel.connect._internal.errors import (
    ConnectCredentialsError,
    ConnectValidationError,
    ConnectWebhookVerificationError,
)
from vercel.connect._internal.models import (
    ConnectAuthorizationDetail,
    ConnectAuthorizationResponse,
    ConnectorMetadata,
    ConnectorRef,
    ConnectTokenResponse,
    ConnectTokenSubject,
    ConnectWebhookClaims,
    DurationInput,
)
from vercel.connect._internal.options import (
    ConnectCredentialsFactory,
    ConnectOptions,
    ConnectServiceOptions,
    _default_async_credentials_factory,
    _default_sync_credentials_factory,
)
from vercel.connect._internal.single_flight import (
    AsyncSingleFlight,
    SingleFlight,
    SyncSingleFlight,
)
from vercel.connect._internal.state import (
    ConnectAuthorizationState,
    ConnectorMetadataState,
    ConnectorRefState,
    ConnectTokenState,
)

if TYPE_CHECKING:
    from vercel._internal.core.session import SdkSession, SyncSdkSession

_SECOND = coerce_duration(1, __import__("datetime").timedelta(seconds=1))
_DETACHED_ENV = "VERCEL_CONNECT_INTERACTIVE_AUTH_MODE"
_LOCAL_HOSTS = ("localhost", "127.0.0.1", "[::1]", "::1")
_DEFAULT_SCOPES_SENTINEL = "*"


def _normalize_scopes(scopes: Sequence[str] | None) -> Sequence[str] | None:
    """Collapse the two spellings of "the connector's defaults" into one.

    `None` and `["*"]` request the same credential, so they must serialize the
    same way and share one cache entry.
    """
    if scopes is None:
        return None
    resolved = list(scopes)
    if resolved == [_DEFAULT_SCOPES_SENTINEL]:
        return None
    return resolved


def _is_local_host(host: str) -> bool:
    host = host.lower()
    return host in _LOCAL_HOSTS or host.endswith(".localhost")


def _validate_return_url(url: str) -> str:
    """Allow https anywhere, and http only on a loopback host."""
    parts = urlsplit(url)
    if parts.scheme == "https" and parts.hostname:
        return url
    if parts.scheme == "http" and parts.hostname and _is_local_host(parts.hostname):
        return url
    raise ConnectValidationError(
        f"return_url must be https, or http on localhost, *.localhost or 127.0.0.1; got {url!r}"
    )


def _validate_webhook_url(url: str) -> str:
    parts = urlsplit(url)
    if parts.scheme != "https" or not parts.hostname:
        raise ConnectValidationError(f"webhook must be an https URL; got {url!r}")
    return url


def is_detached_interactive_auth() -> bool:
    """Whether device-code authorization is the configured default."""
    return os.environ.get(_DETACHED_ENV, "").strip().lower() == "detached"


def _from_epoch(value: Any) -> datetime | None:
    if not isinstance(value, (int, float)):
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc)


def _connector_ref(state: ConnectorRefState) -> ConnectorRef:
    return ConnectorRef(
        id=state.id,
        uid=state.uid,
        type=state.type,
        name=state.name,
        service=state.service,
        service_name=state.service_name,
    )


def _token_response(state: ConnectTokenState) -> ConnectTokenResponse:
    return ConnectTokenResponse(
        token=state.token,
        expires_at=state.expires_at,
        connector=_connector_ref(state.connector),
        token_id=state.token_id,
        name=state.name,
        installation_id=state.installation_id,
        tenant_id=state.tenant_id,
        external_subject=state.external_subject,
        metadata=state.metadata,
        claims=state.claims,
    )


def _authorization_response(state: ConnectAuthorizationState) -> ConnectAuthorizationResponse:
    return ConnectAuthorizationResponse(
        url=state.url,
        request=state.request,
        verifier=state.verifier,
        device_code=state.device_code,
        expires_at=state.expires_at,
        connector=None if state.connector is None else _connector_ref(state.connector),
    )


def _connector_metadata(state: ConnectorMetadataState) -> ConnectorMetadata:
    return ConnectorMetadata(
        id=state.id,
        uid=state.uid,
        type=state.type,
        name=state.name,
        service=state.service,
        client_url=state.client_url,
        created_at=state.created_at,
        updated_at=state.updated_at,
        vendor=state.vendor,
        extra=state.extra,
    )


class ConnectService:
    """Orchestrates credential minting, caching, and authorization flows."""

    def __init__(
        self,
        *,
        api_client: ConnectApiClient,
        options: ConnectServiceOptions,
        cache: TokenCache,
        single_flight: SingleFlight,
        ensure_open: Callable[[], None],
        verify_oidc_token: Callable[..., Any],
        credentials_factory: ConnectCredentialsFactory,
    ) -> None:
        self._api_client = api_client
        self._options = options
        self._cache = cache
        self._credentials_factory = credentials_factory
        self._single_flight = single_flight
        self._ensure_open = ensure_open
        self._verify_oidc_token = verify_oidc_token

    async def _identity(self, options: ConnectOptions | None) -> str:
        supplied = options.vercel_token if options is not None else None
        if supplied is None:
            return await self._credentials_factory()
        if callable(supplied):
            resolved = supplied()
            # Guarded so a sync callable never forces the sync path to suspend.
            # An async callable that genuinely suspends is only usable from the
            # async surface; see ConnectOptions.vercel_token.
            if inspect.isawaitable(resolved):
                resolved = await resolved
            if not isinstance(resolved, str) or not resolved:
                raise ConnectCredentialsError("vercel_token callable returned no token")
            return resolved
        if not supplied:
            raise ConnectCredentialsError("vercel_token was an empty string")
        return supplied

    def _validity_buffer_seconds(self, options: ConnectOptions | None) -> float:
        if options is not None and options.validity_buffer is not None:
            seconds = coerce_duration(options.validity_buffer, _SECOND).total_seconds()
            # A negative buffer would make an already-expired token look usable.
            if seconds < 0:
                raise ConnectValidationError("validity_buffer must not be negative")
            return seconds
        return self._options.validity_buffer.total_seconds()

    async def get_token_response(
        self,
        connector: str,
        *,
        subject: ConnectTokenSubject,
        scopes: Sequence[str] | None = None,
        installation_id: str | None = None,
        audience: Sequence[str] | None = None,
        resources: Sequence[str] | None = None,
        authorization_details: Sequence[ConnectAuthorizationDetail] | None = None,
        options: ConnectOptions | None = None,
    ) -> ConnectTokenResponse:
        self._ensure_open()
        buffer_seconds = self._validity_buffer_seconds(options)
        identity = await self._identity(options)
        scopes = _normalize_scopes(scopes)
        no_cache = options.no_cache if options is not None else False
        force_refresh = options.force_refresh if options is not None else False

        key = build_cache_key(
            connector,
            subject=subject,
            vercel_token=identity,
            scopes=scopes,
            installation_id=installation_id,
            audience=audience,
            resources=resources,
            authorization_details=authorization_details,
        )

        async def load() -> ConnectTokenState:
            # Registered so a concurrent revoke can cancel this load even though
            # nothing is cached under the key yet. The epoch scopes that to loads
            # already running, so a load started after the revoke still caches.
            epoch = self._cache.begin_load(key)
            try:
                state = await self._api_client.create_token(
                    connector,
                    subject=subject,
                    vercel_token=identity,
                    scopes=scopes,
                    installation_id=installation_id,
                    audience=audience,
                    resources=resources,
                    authorization_details=authorization_details,
                )
                if not no_cache:
                    # Dropped when an invalidation landed while this was in flight.
                    self._cache.set(key, state, epoch=epoch)
                return state
            finally:
                self._cache.finish_load(key)

        if no_cache:
            return _token_response(await load())

        def read() -> ConnectTokenState | None:
            return self._cache.get(key, validity_buffer_seconds=buffer_seconds)

        if not force_refresh:
            cached = read()
            if cached is not None:
                return _token_response(cached)

        if force_refresh:
            # Evict up front: if the refetch fails, the caller must not keep being
            # served the credential they just asked to re-validate.
            self._cache.delete(key)
            return _token_response(await load())
        return _token_response(await self._single_flight.run(key, read=read, load=load))

    async def revoke_token(
        self,
        connector: str,
        *,
        subject: ConnectTokenSubject,
        installation_id: str | None = None,
        options: ConnectOptions | None = None,
    ) -> None:
        self._ensure_open()
        identity = await self._identity(options)
        await self._api_client.revoke_token(
            connector,
            subject=subject,
            vercel_token=identity,
            installation_id=installation_id,
        )
        # Scoped, not global: revoking one subject must not evict the others.
        self._cache.delete_by_identity(connector, subject=subject, installation_id=installation_id)

    async def start_authorization(
        self,
        connector: str,
        *,
        subject: ConnectTokenSubject,
        scopes: Sequence[str] | None = None,
        installation_id: str | None = None,
        return_url: str | None = None,
        webhook: str | None = None,
        device_code: bool | None = None,
        expires_in: DurationInput | None = None,
        options: ConnectOptions | None = None,
    ) -> ConnectAuthorizationResponse:
        self._ensure_open()
        if return_url is not None:
            _validate_return_url(return_url)
        if webhook is not None:
            _validate_webhook_url(webhook)

        if device_code is None and is_detached_interactive_auth():
            device_code = True
            if return_url is not None:
                # The TypeScript SDK discards the caller's URL silently.
                warnings.warn(
                    f"{_DETACHED_ENV}=detached forces device-code authorization, so the "
                    "supplied return_url is ignored",
                    UserWarning,
                    stacklevel=3,
                )
                return_url = None

        identity = await self._identity(options)
        state = await self._api_client.create_authorization(
            connector,
            subject=subject,
            vercel_token=identity,
            scopes=_normalize_scopes(scopes),
            installation_id=installation_id,
            return_url=return_url,
            webhook=webhook,
            device_code=device_code,
            expires_in=None if expires_in is None else coerce_duration(expires_in, _SECOND),
        )
        return _authorization_response(state)

    async def get_connector_metadata(
        self,
        connector: str,
        *,
        options: ConnectOptions | None = None,
    ) -> ConnectorMetadata:
        self._ensure_open()
        identity = await self._identity(options)
        state = await self._api_client.get_connector(connector, vercel_token=identity)
        return _connector_metadata(state)

    @staticmethod
    def _resolve_headers(headers: object) -> Mapping[str, str]:
        """Accept a header mapping, or any request object that exposes one."""
        candidate = getattr(headers, "headers", headers)
        if not hasattr(candidate, "items"):
            raise ConnectValidationError(
                "headers must be a mapping of header names to values, or a request "
                f"object exposing one; got {type(headers).__name__}"
            )
        return cast("Mapping[str, str]", candidate)

    async def verify_webhook(
        self,
        headers: object,
        *,
        project_id: str | None = None,
        environment: str | None = None,
        owner_id: str | None = None,
        audience: str | Sequence[str] | None = None,
    ) -> ConnectWebhookClaims:
        from vercel.oidc import verify as oidc_verify

        resolved_headers = self._resolve_headers(headers)
        try:
            token = oidc_verify.extract_bearer_token(resolved_headers)
        except oidc_verify.VercelOidcVerificationError as exc:
            raise ConnectWebhookVerificationError(str(exc)) from exc

        try:
            result = self._verify_oidc_token(
                token,
                project_id=project_id,
                environment=environment,
                owner_id=owner_id,
                audience=audience,
                issuer=self._options.oidc_issuer,
            )
            claims = await result if inspect.isawaitable(result) else result
        except oidc_verify.VercelOidcTokenError as exc:
            raise ConnectWebhookVerificationError(str(exc)) from exc

        raw_audience = claims.get("aud")
        if raw_audience is None:
            resolved_audience: list[str] = []
        elif isinstance(raw_audience, str):
            resolved_audience = [raw_audience]
        else:
            resolved_audience = list(raw_audience)

        return ConnectWebhookClaims(
            issuer=claims.get("iss", ""),
            subject=claims.get("sub", ""),
            project_id=claims.get("project_id"),
            environment=claims.get("environment"),
            owner_id=claims.get("owner_id"),
            audience=resolved_audience,
            issued_at=_from_epoch(claims.get("iat")),
            expires_at=_from_epoch(claims.get("exp")),
            claims=claims,
        )

    def delete_token_cache_entry(
        self,
        connector: str,
        *,
        subject: ConnectTokenSubject,
        installation_id: str | None = None,
    ) -> None:
        self._cache.delete_by_identity(connector, subject=subject, installation_id=installation_id)

    def clear_token_cache(self) -> None:
        self._cache.clear()


def get_connect_service(session: "SdkSession | SyncSdkSession") -> ConnectService:
    """Resolve the Connect service for a session, creating it once per session."""
    from vercel._internal.core.session import SyncSdkSession

    def factory() -> ConnectService:
        options = session.get_service_option(ConnectServiceOptions) or ConnectServiceOptions()
        is_sync = isinstance(session, SyncSdkSession)
        from vercel.oidc import verify as oidc_verify

        def verify_oidc_token(token: str, **kwargs: Any) -> Any:
            # Resolved per call so the mode-appropriate implementation is used,
            # and so tests can substitute it.
            if is_sync:
                return oidc_verify.verify_vercel_oidc_token(token, **kwargs)
            return oidc_verify.verify_vercel_oidc_token_async(token, **kwargs)

        credentials_factory = options.credentials_factory or (
            _default_sync_credentials_factory if is_sync else _default_async_credentials_factory
        )
        return ConnectService(
            api_client=ConnectApiClient(
                base_url=options.base_url,
                credentials_factory=credentials_factory,
                transport=session.get_transport(),
                timeout=options.timeout,
            ),
            options=options,
            cache=TokenCache(max_size=options.token_cache_size),
            single_flight=SyncSingleFlight() if is_sync else AsyncSingleFlight(),
            ensure_open=session.check_open,
            verify_oidc_token=verify_oidc_token,
            credentials_factory=credentials_factory,
        )

    return session.get_or_create_service(ConnectService, factory)


__all__ = ["ConnectService", "get_connect_service", "is_detached_interactive_auth"]
