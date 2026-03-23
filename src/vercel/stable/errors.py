"""Public error types for the stable client surface."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


class VercelError(Exception):
    """Base class for stable client errors."""


class APIResponseError(VercelError):
    """Raised when the API returns an unexpected error response."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        error_code: str | None = None,
        request_id: str | None = None,
        trace_id: str | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code
        self.request_id = request_id
        self.trace_id = trace_id
        self.payload = dict(payload) if payload is not None else None


class AuthenticationError(APIResponseError):
    """Raised when authentication fails."""


class AuthorizationError(APIResponseError):
    """Raised when the caller is not authorized."""


class NotFoundError(APIResponseError):
    """Raised when a requested resource does not exist."""


class ConflictError(APIResponseError):
    """Raised when a request conflicts with existing state."""


class RateLimitError(APIResponseError):
    """Raised when the API rejects a request due to rate limiting."""


class TransportClosedError(VercelError):
    """Raised when a closed client lineage is reused."""


__all__ = [
    "VercelError",
    "AuthenticationError",
    "AuthorizationError",
    "NotFoundError",
    "ConflictError",
    "RateLimitError",
    "APIResponseError",
    "TransportClosedError",
]
