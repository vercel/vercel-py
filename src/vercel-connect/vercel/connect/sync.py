"""Sync mirror for the Vercel Connect SDK surface.

Every name here matches `vercel.connect`, with no `await`.

These functions are safe to call from inside a running event loop: the sync
transport never suspends, so nothing deadlocks. They do block the loop for the
duration of the request, so prefer the async surface in async code. Calling them
inside an `async with vercel.api.session(...)` block is rejected outright, since
mixing modes in one session is a bug.
"""

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from vercel._internal.core.session import get_active_sync_session
from vercel.connect._internal.errors import (
    ConnectApiError,
    ConnectCredentialsError,
    ConnectError,
    ConnectorInstallationRequiredError,
    ConnectResponseError,
    ConnectValidationError,
    ConnectWebhookVerificationError,
    NoValidTokenError,
    UserAuthorizationRequiredError,
)
from vercel.connect._internal.models import (
    ConnectAppTokenSubject,
    ConnectAuthorizationDetail,
    ConnectAuthorizationResponse,
    ConnectCustomAuthorizationDetail,
    ConnectGitHubAppInstallationAuthorizationDetail,
    ConnectJwtBearerTokenSubject,
    ConnectorMetadata,
    ConnectorRef,
    ConnectSubjectType,
    ConnectTokenExchangeSubject,
    ConnectTokenResponse,
    ConnectTokenSubject,
    ConnectUserTokenSubject,
    ConnectWebhookClaims,
    DurationInput,
)
from vercel.connect._internal.options import (
    ConnectOptions,
    ConnectServiceOptions,
    VercelTokenInput,
)
from vercel.connect._internal.service import ConnectService, get_connect_service
from vercel.connect._internal.sync_runtime import (
    get_connector_metadata as _get_connector_metadata,
    get_token as _get_token,
    get_token_response as _get_token_response,
    revoke_token as _revoke_token,
    start_authorization as _start_authorization,
    verify_connect_webhook as _verify_connect_webhook,
)
from vercel.connect.version import __version__


def _service() -> ConnectService:
    return get_connect_service(get_active_sync_session())


def get_token(
    connector: str,
    *,
    subject: ConnectTokenSubject,
    scopes: Sequence[str] | None = None,
    installation_id: str | None = None,
    audience: Sequence[str] | None = None,
    resources: Sequence[str] | None = None,
    authorization_details: Sequence[ConnectAuthorizationDetail] | None = None,
    options: ConnectOptions | None = None,
) -> str:
    """Mint a short-lived upstream credential.

    Args:
        connector: Connector id (`scl_...`) or UID (`slack/my-bot`).
        subject: Whose authority the token carries.
        scopes: Requested scopes. Omit for the connector's defaults for this
            subject type.
        installation_id: Select one installation when the connector has several.
        audience: Optional token audience.
        resources: Optional resource indicators (RFC 8707).
        authorization_details: Optional rich authorization requests (RFC 9396).
        options: Per-call overrides such as an explicit platform identity token
            or a forced cache bypass.

    Returns:
        The credential to send to the upstream provider.

    Raises:
        UserAuthorizationRequiredError: If this user has not consented yet. Call
            `start_authorization` and send them to the returned URL.
        ConnectorInstallationRequiredError: If the connector is not installed
            for the target tenant.
        NoValidTokenError: If the grant exists but no usable credential can be
            issued.
        ConnectApiError: For any other Connect API failure.
    """
    return _get_token(
        _service(),
        connector,
        subject=subject,
        scopes=scopes,
        installation_id=installation_id,
        audience=audience,
        resources=resources,
        authorization_details=authorization_details,
        options=options,
    )


def get_token_response(
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
    """Mint a credential and return the full response envelope.

    Identical to `get_token`, but also surfaces the per-issuance `token_id`
    (`stk_...`) for correlating with Vercel observability, the expiry, the
    issuing connector, the installation and tenant, and any driver metadata or
    allow-listed upstream claims.

    Args:
        connector: Connector id (`scl_...`) or UID (`slack/my-bot`).
        subject: Whose authority the token carries.
        scopes: Requested scopes. Omit for the connector's defaults.
        installation_id: Select one installation when the connector has several.
        audience: Optional token audience.
        resources: Optional resource indicators (RFC 8707).
        authorization_details: Optional rich authorization requests (RFC 9396).
        options: Per-call overrides.

    Returns:
        The credential and its issuance metadata.

    Raises:
        UserAuthorizationRequiredError: If this user has not consented yet.
        ConnectorInstallationRequiredError: If the connector is not installed.
        NoValidTokenError: If no usable credential can be issued.
        ConnectApiError: For any other Connect API failure.
    """
    return _get_token_response(
        _service(),
        connector,
        subject=subject,
        scopes=scopes,
        installation_id=installation_id,
        audience=audience,
        resources=resources,
        authorization_details=authorization_details,
        options=options,
    )


def revoke_token(
    connector: str,
    *,
    subject: ConnectTokenSubject,
    installation_id: str | None = None,
    options: ConnectOptions | None = None,
) -> None:
    """Revoke a grant upstream and drop its cached credentials.

    This removes the stored authorization server-side, not just the local cache
    entry. Use it when a user disconnects an integration or a tenant is
    offboarded.

    Args:
        connector: Connector id or UID.
        subject: The subject whose grant should be revoked.
        installation_id: Restrict revocation to one installation.
        options: Per-call overrides.

    Raises:
        ConnectApiError: If the revocation request fails.
    """
    _revoke_token(
        _service(),
        connector,
        subject=subject,
        installation_id=installation_id,
        options=options,
    )


def start_authorization(
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
    """Start an end-user consent flow and get a URL to send them to.

    Connect owns the OAuth client, PKCE, state, and the callback handshake, so
    you never implement the dance. Pass `return_url` for a web app and Connect
    redirects back after consent. Pass `device_code=True` for a CLI or headless
    process: Connect returns a short code the user approves elsewhere, and you
    poll `get_token(..., options=ConnectOptions(force_refresh=True))` until it
    succeeds, because nothing is delivered back to your process.

    Setting `VERCEL_CONNECT_INTERACTIVE_AUTH_MODE=detached` makes device-code the
    default; a warning is emitted if that silently discards a `return_url` you
    supplied.

    Args:
        connector: Connector id or UID.
        subject: The subject to authorize, usually a user subject.
        scopes: Requested scopes. Omit for the connector's defaults.
        installation_id: Target one installation.
        return_url: Where Connect should redirect after consent. Must be
            `https://`, or `http://` on localhost or 127.0.0.1.
        webhook: An `https://` URL to notify.
        device_code: Return a device code instead of redirecting.
        expires_in: How long the authorization request stays valid.
        options: Per-call overrides.

    Returns:
        The consent URL, plus the opaque `request` and `verifier` values and an
        optional device code.

    Raises:
        ConnectValidationError: If `return_url` or `webhook` is not an allowed
            URL.
        ConnectApiError: If the authorization request fails.
    """
    return _start_authorization(
        _service(),
        connector,
        subject=subject,
        scopes=scopes,
        installation_id=installation_id,
        return_url=return_url,
        webhook=webhook,
        device_code=device_code,
        expires_in=expires_in,
        options=options,
    )


def get_connector_metadata(
    connector: str,
    *,
    options: ConnectOptions | None = None,
) -> ConnectorMetadata:
    """Read a connector's identity and configuration.

    Read this instead of hardcoding assumptions about which scopes or subject
    types a connector supports. Provider secrets are redacted by the server.
    Fields outside the documented set are preserved in `extra`.

    Args:
        connector: Connector id or UID.
        options: Per-call overrides.

    Returns:
        The connector's metadata.

    Raises:
        ConnectApiError: If the connector cannot be read.
    """
    return _get_connector_metadata(_service(), connector, options=options)


def verify_connect_webhook(
    headers: Mapping[str, str] | Any,
    *,
    project_id: str | None = None,
    environment: str | None = None,
    owner_id: str | None = None,
    audience: str | Sequence[str] | None = None,
) -> ConnectWebhookClaims:
    """Verify an inbound Connect trigger request.

    A connector with triggers enabled receives provider webhooks and forwards
    them to your project with a Vercel OIDC token as the `Authorization` bearer.
    So instead of implementing a different signature scheme per provider, verify
    one thing.

    Trust boundary: this accepts *any* valid Vercel OIDC token for this project
    and environment. It is not pinned to a specific connector or deployment.

    Verification pins the issuer to `https://oidc.vercel.com`, allows only RS256,
    and resolves the signing key by `kid` from Vercel's JWKS. It **fails closed**:
    if the expected project or environment cannot be determined from the
    arguments or the environment, every request is rejected.

    Args:
        headers: Inbound request headers, or any request object exposing a
            `headers` mapping (httpx, Starlette, FastAPI, Django). Only
            `Authorization` is read.
        project_id: Expected project. Defaults to `VERCEL_PROJECT_ID`.
        environment: Expected environment. Defaults to `VERCEL_TARGET_ENV`, then
            `VERCEL_ENV`.
        owner_id: Expected team owner, required when the token's project claim is
            a wildcard.
        audience: Expected audience.

    Returns:
        The verified claims.

    Raises:
        ConnectWebhookVerificationError: If the header is missing or malformed,
            the signature does not verify, a claim does not match, or the
            expected project and environment cannot be resolved.
    """
    return _verify_connect_webhook(
        _service(),
        headers,
        project_id=project_id,
        environment=environment,
        owner_id=owner_id,
        audience=audience,
    )


def create_connect_webhook_verifier(
    *,
    project_id: str | None = None,
    environment: str | None = None,
    owner_id: str | None = None,
    audience: str | Sequence[str] | None = None,
) -> Callable[[Mapping[str, str]], ConnectWebhookClaims]:
    """Build a reusable webhook verifier bound to a set of expectations.

    Args:
        project_id: Expected project. Defaults to `VERCEL_PROJECT_ID`.
        environment: Expected environment. Defaults to `VERCEL_TARGET_ENV`, then
            `VERCEL_ENV`.
        owner_id: Expected team owner.
        audience: Expected audience.

    Returns:
        A callable taking request headers and returning verified claims.
    """

    def verify(headers: Mapping[str, str]) -> ConnectWebhookClaims:
        return verify_connect_webhook(
            headers,
            project_id=project_id,
            environment=environment,
            owner_id=owner_id,
            audience=audience,
        )

    return verify


def delete_token_cache_entry(
    connector: str,
    *,
    subject: ConnectTokenSubject,
    installation_id: str | None = None,
) -> None:
    """Drop cached credentials for one connector, subject, and installation.

    Eviction is by identity, not by reconstructing the original request
    parameters. If a provider returns 401, evict and retry once: that is cheaper
    than forcing a refresh on every call.

    Args:
        connector: Connector id or UID.
        subject: The subject whose cached credentials should be dropped.
        installation_id: Restrict eviction to one installation.
    """
    _service().delete_token_cache_entry(
        connector,
        subject=subject,
        installation_id=installation_id,
    )


def clear_token_cache() -> None:
    """Drop every cached credential for the active session."""
    _service().clear_token_cache()


__all__ = [
    "ConnectApiError",
    "ConnectAppTokenSubject",
    "ConnectAuthorizationDetail",
    "ConnectAuthorizationResponse",
    "ConnectCredentialsError",
    "ConnectCustomAuthorizationDetail",
    "ConnectError",
    "ConnectGitHubAppInstallationAuthorizationDetail",
    "ConnectJwtBearerTokenSubject",
    "ConnectOptions",
    "ConnectResponseError",
    "ConnectService",
    "ConnectServiceOptions",
    "ConnectSubjectType",
    "ConnectTokenExchangeSubject",
    "ConnectTokenResponse",
    "ConnectTokenSubject",
    "ConnectUserTokenSubject",
    "ConnectValidationError",
    "ConnectWebhookClaims",
    "ConnectWebhookVerificationError",
    "ConnectorInstallationRequiredError",
    "ConnectorMetadata",
    "ConnectorRef",
    "DurationInput",
    "NoValidTokenError",
    "UserAuthorizationRequiredError",
    "VercelTokenInput",
    "__version__",
    "clear_token_cache",
    "create_connect_webhook_verifier",
    "delete_token_cache_entry",
    "get_connect_service",
    "get_connector_metadata",
    "get_token",
    "get_token_response",
    "revoke_token",
    "start_authorization",
    "verify_connect_webhook",
]
