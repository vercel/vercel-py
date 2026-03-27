"""Sandbox API error types."""

from __future__ import annotations

from typing import Any

import httpx


def _normalize_retry_after(value: str | int | None) -> int | str | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


class SandboxError(Exception):
    """Base class for sandbox-specific errors."""


class APIError(SandboxError):
    def __init__(self, response: httpx.Response, message: str, *, data: Any | None = None):
        super().__init__(message)
        self.response = response
        self.status_code = response.status_code
        self.data = data


class SandboxAuthError(APIError):
    """Authentication failures returned by the sandbox API."""


class SandboxPermissionError(APIError):
    """Authorization failures returned by the sandbox API."""


class SandboxRateLimitError(APIError):
    def __init__(
        self,
        response: httpx.Response,
        message: str,
        *,
        data: Any | None = None,
        retry_after: str | int | None = None,
    ) -> None:
        super().__init__(response, message, data=data)
        self.retry_after: int | str | None = _normalize_retry_after(retry_after)


class SandboxServerError(APIError):
    """5xx responses returned by the sandbox API."""


__all__ = [
    "SandboxError",
    "APIError",
    "SandboxAuthError",
    "SandboxPermissionError",
    "SandboxRateLimitError",
    "SandboxServerError",
]
