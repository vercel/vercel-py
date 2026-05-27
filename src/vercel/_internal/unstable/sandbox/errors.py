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


class SandboxResponseError(SandboxError):
    """Raised when a successful Sandbox v2 API response is malformed."""

    def __init__(self, message: str, *, data: object | None = None) -> None:
        super().__init__(message)
        self.data = data


class SandboxCredentialsError(SandboxError):
    """Raised when Sandbox credentials cannot be resolved."""
