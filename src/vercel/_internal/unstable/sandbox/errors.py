"""Sandbox errors for the experimental SDK surface."""

from vercel._internal.unstable.errors import VercelError


class SandboxError(VercelError):
    """Base error for unstable Sandbox operations."""


class SandboxInvalidHandleError(SandboxError):
    """Raised when a Sandbox handle is used after invalidation."""


class SandboxTerminalStateError(SandboxError):
    """Raised when Sandbox creation reaches a terminal state."""


class SandboxApiError(SandboxError):
    """Raised when the Sandbox v2 API returns an error response."""
