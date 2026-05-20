from __future__ import annotations

import pytest
from hypothesis import given, strategies as st

from vercel._internal.unstable.errors import CredentialProviderError, CredentialResolutionError
from vercel._internal.unstable.sandbox.lifecycle import (
    is_ready_for_create,
    is_terminal_for_create,
)
from vercel.unstable import VercelError
from vercel.unstable.sandbox import (
    SandboxAPIError,
    SandboxError,
    SandboxOperationTimeoutError,
    SandboxStatus,
    SandboxTerminalStateError,
)


def test_unstable_error_hierarchy_inherits_from_vercel_error() -> None:
    assert issubclass(CredentialResolutionError, VercelError)
    assert issubclass(CredentialProviderError, VercelError)
    assert issubclass(SandboxError, VercelError)
    assert issubclass(SandboxAPIError, SandboxError)
    assert issubclass(SandboxOperationTimeoutError, SandboxError)
    assert issubclass(SandboxTerminalStateError, SandboxError)


@given(st.one_of(st.none(), st.sampled_from(list(SandboxStatus))))
def test_lifecycle_status_predicates(status: SandboxStatus | None) -> None:
    assert is_ready_for_create(status) is (status is SandboxStatus.RUNNING)
    assert is_terminal_for_create(status) is (
        status
        in {
            SandboxStatus.FAILED,
            SandboxStatus.ABORTED,
            SandboxStatus.STOPPED,
            SandboxStatus.STOPPING,
        }
    )


@given(st.integers(min_value=0, max_value=86_400))
def test_sandbox_api_error_normalizes_numeric_retry_after(value: int) -> None:
    response = object()
    error = SandboxAPIError(
        "rate limited",
        response=response,
        status_code=429,
        data={"error": {"code": "rate_limited"}},
        retry_after=str(value),
    )

    assert isinstance(error, SandboxError)
    assert isinstance(error, VercelError)
    assert error.response is response
    assert error.status_code == 429
    assert error.data == {"error": {"code": "rate_limited"}}
    assert error.retry_after == value


@pytest.mark.parametrize("value", [None, "", "tomorrow", "1.5", "10 seconds"])
def test_sandbox_api_error_ignores_non_numeric_retry_after(value: str | None) -> None:
    error = SandboxAPIError(
        "rate limited",
        response=object(),
        status_code=429,
        retry_after=value,
    )

    assert error.retry_after is None
