"""Test that non-2xx streaming responses raise sandbox-specific errors."""

import httpx
import pytest
import respx

from vercel.sandbox import (
    APIError,
    SandboxAuthError,
    SandboxError,
    SandboxNotFoundError,
    SandboxPermissionError,
    SandboxRateLimitError,
    SandboxServerError,
)

SANDBOX_API_BASE = "https://api.vercel.com"
SANDBOX_ID = "sbx_test123"
CMD_ID = "cmd_test456"


def _sync_client(**kwargs):
    from vercel._internal.sandbox.core import SyncSandboxOpsClient

    return SyncSandboxOpsClient(**kwargs)


def _async_client(**kwargs):
    from vercel._internal.sandbox.core import AsyncSandboxOpsClient

    return AsyncSandboxOpsClient(**kwargs)


def _make_error_response(
    status_code: int,
    *,
    code: str,
    message: str,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    return httpx.Response(
        status_code,
        headers=headers,
        json={"error": {"code": code, "message": message}},
    )


SYNC_CASES = [
    (404, SandboxNotFoundError, "not_found", "Missing command.", None, None),
    (401, SandboxAuthError, "unauthorized", "Authentication required.", None, None),
    (403, SandboxPermissionError, "forbidden", "Access denied.", None, None),
    (429, SandboxRateLimitError, "rate_limited", "Slow down.", {"retry-after": "120"}, 120),
    (500, SandboxServerError, "internal_server_error", "Something broke.", None, None),
]


class TestStreamingErrorsSync:
    """Sync get_logs raises sandbox-specific errors on non-2xx responses."""

    @respx.mock
    @pytest.mark.parametrize(
        "status_code,error_type,code,message,headers,retry_after",
        SYNC_CASES,
    )
    def test_get_logs_raises_specific_error(
        self,
        status_code,
        error_type,
        code,
        message,
        headers,
        retry_after,
    ):
        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{SANDBOX_ID}/cmd/{CMD_ID}/logs").mock(
            return_value=_make_error_response(
                status_code,
                code=code,
                message=message,
                headers=headers,
            )
        )

        client = _sync_client(host=SANDBOX_API_BASE, team_id="team_test", token="tok")
        try:
            with pytest.raises(error_type) as exc_info:
                list(client.get_logs(sandbox_id=SANDBOX_ID, cmd_id=CMD_ID))
        finally:
            client.close()

        error = exc_info.value
        assert type(error) is error_type
        assert isinstance(error, APIError)
        assert isinstance(error, SandboxError)
        assert error.status_code == status_code
        assert error.data == {"error": {"code": code, "message": message}}
        assert message in str(error)
        if retry_after is not None:
            assert getattr(error, "retry_after", None) == retry_after


class TestStreamingErrorsAsync:
    """Async get_logs raises sandbox-specific errors on non-2xx responses."""

    @respx.mock
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status_code,error_type,code,message,headers,retry_after",
        SYNC_CASES,
    )
    async def test_get_logs_raises_specific_error(
        self,
        status_code,
        error_type,
        code,
        message,
        headers,
        retry_after,
    ):
        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{SANDBOX_ID}/cmd/{CMD_ID}/logs").mock(
            return_value=_make_error_response(
                status_code,
                code=code,
                message=message,
                headers=headers,
            )
        )

        client = _async_client(host=SANDBOX_API_BASE, team_id="team_test", token="tok")
        try:
            with pytest.raises(error_type) as exc_info:
                async for _ in client.get_logs(sandbox_id=SANDBOX_ID, cmd_id=CMD_ID):
                    pass
        finally:
            await client.aclose()

        error = exc_info.value
        assert type(error) is error_type
        assert isinstance(error, APIError)
        assert isinstance(error, SandboxError)
        assert error.status_code == status_code
        assert error.data == {"error": {"code": code, "message": message}}
        assert message in str(error)
        if retry_after is not None:
            assert getattr(error, "retry_after", None) == retry_after
