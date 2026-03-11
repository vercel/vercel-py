"""Private error helpers for the stable client surface."""

from __future__ import annotations

from vercel.stable.errors import (
    APIResponseError,
    AuthenticationError,
    AuthorizationError,
    ConflictError,
    NotFoundError,
    RateLimitError,
    VercelError,
)


def error_for_status(status_code: int, message: str) -> VercelError:
    if status_code == 401:
        return AuthenticationError(message)
    if status_code == 403:
        return AuthorizationError(message)
    if status_code == 404:
        return NotFoundError(message)
    if status_code == 409:
        return ConflictError(message)
    if status_code == 429:
        return RateLimitError(message)
    return APIResponseError(message, status_code=status_code)


__all__ = ["error_for_status"]
