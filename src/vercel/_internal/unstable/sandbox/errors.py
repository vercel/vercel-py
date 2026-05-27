"""Sandbox errors for the experimental SDK surface."""

from typing import TYPE_CHECKING

import httpx

from vercel._internal.unstable.errors import VercelError

if TYPE_CHECKING:
    from vercel._internal.unstable.sandbox.models import SandboxStatus


class SandboxError(VercelError):
    """Base error for unstable Sandbox operations."""


class SandboxInvalidHandleError(SandboxError):
    """Raised when a Sandbox handle is used after invalidation."""


class SandboxTerminalStateError(SandboxError):
    """Raised when Sandbox creation reaches a terminal state."""

    def __init__(
        self,
        message: str,
        *,
        status: "SandboxStatus",
        sandbox: object | None = None,
    ) -> None:
        super().__init__(message)
        self.status: SandboxStatus = status
        self.sandbox = sandbox


class SandboxCleanupError(SandboxError):
    """Raised when context-managed Sandbox resource cleanup fails."""

    def __init__(
        self,
        message: str,
        *,
        resource_type: str,
        resource_id: str,
        cause: BaseException,
    ) -> None:
        super().__init__(message)
        self.resource_type = resource_type
        self.resource_id = resource_id
        self.cause = cause


class SandboxApiError(SandboxError):
    """Raised when the Sandbox v2 API returns an error response."""

    def __init__(
        self,
        response: httpx.Response,
        message: str,
        *,
        data: object | None = None,
    ) -> None:
        super().__init__(message)
        self.response = response
        self.status_code = response.status_code
        self.data = data
        self.code = _extract_api_error_code(data)


class SandboxResponseError(SandboxError):
    """Raised when a successful Sandbox v2 API response is malformed."""

    def __init__(self, message: str, *, data: object | None = None) -> None:
        super().__init__(message)
        self.data = data


class SandboxCredentialsError(SandboxError):
    """Raised when Sandbox credentials cannot be resolved."""


def _extract_api_error_code(data: object | None) -> str | None:
    if not isinstance(data, dict):
        return None
    error = data.get("error")
    if not isinstance(error, dict):
        return None
    code = error.get("code")
    if not isinstance(code, str):
        return None
    return code
