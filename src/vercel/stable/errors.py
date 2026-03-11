"""Public error types for the stable client surface."""

from __future__ import annotations


class VercelError(Exception):
    """Base class for stable client errors."""


class AuthenticationError(VercelError):
    """Raised when authentication fails."""


class AuthorizationError(VercelError):
    """Raised when the caller is not authorized."""


class NotFoundError(VercelError):
    """Raised when a requested resource does not exist."""


class ConflictError(VercelError):
    """Raised when a request conflicts with existing state."""


class RateLimitError(VercelError):
    """Raised when the API rejects a request due to rate limiting."""


class TransportClosedError(VercelError):
    """Raised when a closed client lineage is reused."""


class APIResponseError(VercelError):
    """Raised when the API returns an unexpected error response."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


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
