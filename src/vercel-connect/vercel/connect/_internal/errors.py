"""Connect errors for the SDK surface."""

import httpx

from vercel._internal.core.errors import VercelError
from vercel.connect._internal.models import JSONObject


class ConnectError(VercelError):
    """Base error for Vercel Connect operations."""


class ConnectApiError(ConnectError):
    """Raised when the Connect API returns an error response.

    Attributes:
        response: The raw httpx response.
        status_code: HTTP status code.
        status_text: HTTP reason phrase.
        code: Server-supplied error code, when present. Error classes are chosen
            from this code, never from the status code or the message text.
        vendor: The upstream provider's own error payload, when Connect forwards
            one. Useful when the provider rejects something Connect cannot
            interpret.
        request_id: The `x-vercel-id` response header, for correlating a failure
            with Vercel-side observability.
        data: The parsed error body, when it was JSON.
    """

    def __init__(
        self,
        response: httpx.Response,
        message: str,
        *,
        code: str | None = None,
        vendor: JSONObject | None = None,
        data: object | None = None,
    ) -> None:
        super().__init__(message)
        self.response = response
        self.status_code = response.status_code
        self.status_text = response.reason_phrase
        self.code = code
        self.vendor = vendor
        self.request_id = response.headers.get("x-vercel-id")
        self.data = data

    def __str__(self) -> str:
        # This is the only layer that formats code and status, so callers never
        # see them twice.
        details = []
        if self.code:
            details.append(f"code={self.code}")
        details.append(f"status={self.status_code}")
        if self.request_id:
            details.append(f"request_id={self.request_id}")
        message = super().__str__()
        rendered_details = f"({', '.join(details)})"
        return f"{message} {rendered_details}" if message else rendered_details


class NoValidTokenError(ConnectApiError):
    """Raised when a grant existed but no usable credential can be issued.

    Server code `no_token`. The grant is gone or unusable; the subject must
    authorize again.
    """


class UserAuthorizationRequiredError(ConnectApiError):
    """Raised when this user has not yet consented.

    Server code `user_authorization_required`. This is a recoverable state, not a
    bug: call `start_authorization` and send the user to the returned URL.
    """


class ConnectorInstallationRequiredError(ConnectApiError):
    """Raised when the connector is not installed for the target tenant.

    Server codes `client_installation_required` and
    `connector_installation_required`. An operator must install the connector
    from the Vercel dashboard or CLI. This is a configuration problem, not a
    per-user one.
    """


class ConnectResponseError(ConnectError):
    """Raised when a successful Connect API response is malformed."""

    def __init__(self, message: str, *, data: object | None = None) -> None:
        super().__init__(message)
        self.data = data


class ConnectCredentialsError(ConnectError):
    """Raised when the deployment's Vercel OIDC token cannot be resolved."""


class ConnectValidationError(ConnectError):
    """Raised when caller-supplied arguments are rejected before any request."""


class ConnectWebhookVerificationError(ConnectError):
    """Raised when an inbound Connect trigger request cannot be verified.

    Verification fails closed: a missing bearer token, an unverifiable
    signature, or an unresolvable expected project/environment all raise.
    """


__all__ = [
    "ConnectApiError",
    "ConnectCredentialsError",
    "ConnectError",
    "ConnectResponseError",
    "ConnectValidationError",
    "ConnectWebhookVerificationError",
    "ConnectorInstallationRequiredError",
    "NoValidTokenError",
    "UserAuthorizationRequiredError",
]
