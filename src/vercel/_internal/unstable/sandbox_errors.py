"""Error adapters for the unstable Sandbox boundary."""

from __future__ import annotations

from vercel._internal.sandbox.errors import (
    APIError as StableSandboxAPIError,
    SandboxRateLimitError as StableSandboxRateLimitError,
)


def normalize_retry_after(value: str | int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def sandbox_api_error_context(
    error: StableSandboxAPIError,
) -> tuple[str, object, int, object | None, int | None]:
    retry_after = None
    if isinstance(error, StableSandboxRateLimitError):
        retry_after = error.retry_after
    return (
        str(error),
        error.response,
        error.status_code,
        error.data,
        normalize_retry_after(retry_after),
    )


__all__ = ["normalize_retry_after", "sandbox_api_error_context"]
