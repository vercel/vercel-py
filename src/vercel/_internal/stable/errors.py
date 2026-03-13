"""Private error helpers for the stable client surface."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from vercel.stable.errors import (
    APIResponseError,
    AuthenticationError,
    AuthorizationError,
    ConflictError,
    NotFoundError,
    RateLimitError,
    VercelError,
)


@dataclass(frozen=True, slots=True)
class ErrorDetails:
    message: str
    error_code: str | None = None
    request_id: str | None = None
    trace_id: str | None = None
    payload: Mapping[str, Any] | None = None


def error_for_status(status_code: int, details: ErrorDetails) -> VercelError:
    if status_code == 401:
        return AuthenticationError(
            details.message,
            status_code=status_code,
            error_code=details.error_code,
            request_id=details.request_id,
            trace_id=details.trace_id,
            payload=details.payload,
        )
    if status_code == 403:
        return AuthorizationError(
            details.message,
            status_code=status_code,
            error_code=details.error_code,
            request_id=details.request_id,
            trace_id=details.trace_id,
            payload=details.payload,
        )
    if status_code == 404:
        return NotFoundError(
            details.message,
            status_code=status_code,
            error_code=details.error_code,
            request_id=details.request_id,
            trace_id=details.trace_id,
            payload=details.payload,
        )
    if status_code == 409:
        return ConflictError(
            details.message,
            status_code=status_code,
            error_code=details.error_code,
            request_id=details.request_id,
            trace_id=details.trace_id,
            payload=details.payload,
        )
    if status_code == 429:
        return RateLimitError(
            details.message,
            status_code=status_code,
            error_code=details.error_code,
            request_id=details.request_id,
            trace_id=details.trace_id,
            payload=details.payload,
        )
    return APIResponseError(
        details.message,
        status_code=status_code,
        error_code=details.error_code,
        request_id=details.request_id,
        trace_id=details.trace_id,
        payload=details.payload,
    )


__all__ = ["ErrorDetails", "error_for_status"]
