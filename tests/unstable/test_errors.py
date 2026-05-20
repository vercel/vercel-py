from __future__ import annotations

import httpx

from vercel._internal.sandbox.errors import APIError, SandboxRateLimitError
from vercel._internal.unstable.errors import CredentialProviderError, CredentialResolutionError
from vercel.unstable import VercelError
from vercel.unstable.sandbox import (
    SandboxAPIError,
    SandboxError,
    SandboxOperationTimeoutError,
    SandboxTerminalStateError,
)


def test_unstable_error_hierarchy_inherits_from_vercel_error() -> None:
    assert issubclass(CredentialResolutionError, VercelError)
    assert issubclass(CredentialProviderError, VercelError)
    assert issubclass(SandboxError, VercelError)
    assert issubclass(SandboxAPIError, SandboxError)
    assert issubclass(SandboxOperationTimeoutError, SandboxError)
    assert issubclass(SandboxTerminalStateError, SandboxError)


def test_sandbox_api_error_translation_preserves_context() -> None:
    request = httpx.Request("POST", "https://api.vercel.test/v2/sandboxes")
    response = httpx.Response(429, request=request, headers={"retry-after": "120"})
    stable_error = SandboxRateLimitError(
        response,
        "rate limited",
        data={"error": {"code": "rate_limited"}},
        retry_after=response.headers.get("retry-after"),
    )

    error = SandboxAPIError.from_stable_error(stable_error)

    assert isinstance(error, SandboxError)
    assert isinstance(error, VercelError)
    assert error.response is response
    assert error.status_code == 429
    assert error.data == {"error": {"code": "rate_limited"}}
    assert error.retry_after == 120
    assert str(error) == "rate limited"


def test_sandbox_api_error_translation_handles_non_rate_limit_errors() -> None:
    request = httpx.Request("GET", "https://api.vercel.test/v1/sandboxes/sbx_123")
    response = httpx.Response(500, request=request)
    stable_error = APIError(response, "server failed", data={"error": "failed"})

    error = SandboxAPIError.from_stable_error(stable_error)

    assert error.response is response
    assert error.status_code == 500
    assert error.data == {"error": "failed"}
    assert error.retry_after is None


def test_sandbox_api_error_translation_ignores_non_numeric_retry_after() -> None:
    request = httpx.Request("POST", "https://api.vercel.test/v2/sandboxes")
    response = httpx.Response(429, request=request, headers={"retry-after": "tomorrow"})
    stable_error = SandboxRateLimitError(
        response,
        "rate limited",
        retry_after=response.headers.get("retry-after"),
    )

    error = SandboxAPIError.from_stable_error(stable_error)

    assert error.retry_after is None
