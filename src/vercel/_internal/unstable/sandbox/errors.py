"""Internal Sandbox errors for unstable APIs."""

from __future__ import annotations

from vercel._internal.unstable.errors import VercelError


def _normalize_retry_after(value: str | int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class SandboxError(VercelError):
    """Base class for unstable Sandbox errors."""


class SandboxAPIError(SandboxError):
    """Wraps Sandbox API failures at the unstable boundary."""

    def __init__(
        self,
        message: str,
        *,
        response: object,
        status_code: int,
        data: object | None = None,
        retry_after: str | int | None = None,
    ) -> None:
        super().__init__(message)
        self.response = response
        self.status_code = status_code
        self.data = data
        self.retry_after: int | None = _normalize_retry_after(retry_after)


class SandboxOperationTimeoutError(SandboxError):
    """Raised when a Sandbox operation exceeds its whole-operation deadline."""


class SandboxTerminalStateError(SandboxError):
    """Raised when a Sandbox operation reaches a terminal failure state."""


__all__ = [
    "SandboxAPIError",
    "SandboxError",
    "SandboxOperationTimeoutError",
    "SandboxTerminalStateError",
]
