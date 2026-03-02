"""Test that non-2xx streaming responses raise APIError."""

import httpx
import pytest
import respx

from vercel.sandbox import APIError

SANDBOX_API_BASE = "https://api.vercel.com"
SANDBOX_ID = "sbx_test123"
CMD_ID = "cmd_test456"


def _sync_client(**kwargs):
    from vercel._internal.sandbox.core import SyncSandboxOpsClient

    return SyncSandboxOpsClient(**kwargs)


def _async_client(**kwargs):
    from vercel._internal.sandbox.core import AsyncSandboxOpsClient

    return AsyncSandboxOpsClient(**kwargs)


class TestStreamingErrorsSync:
    """Sync get_logs raises APIError on non-2xx responses."""

    @respx.mock
    def test_get_logs_raises_on_401(self):
        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{SANDBOX_ID}/cmd/{CMD_ID}/logs").mock(
            return_value=httpx.Response(
                401,
                json={"error": {"code": "unauthorized", "message": "Authentication required."}},
            )
        )

        client = _sync_client(host=SANDBOX_API_BASE, team_id="team_test", token="bad_token")
        with pytest.raises(APIError) as exc_info:
            list(client.get_logs(sandbox_id=SANDBOX_ID, cmd_id=CMD_ID))

        assert exc_info.value.status_code == 401
        client.close()

    @respx.mock
    def test_get_logs_raises_on_500(self):
        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{SANDBOX_ID}/cmd/{CMD_ID}/logs").mock(
            return_value=httpx.Response(
                500,
                json={"error": {"code": "internal_server_error", "message": "Something broke."}},
            )
        )

        client = _sync_client(host=SANDBOX_API_BASE, team_id="team_test", token="tok")
        with pytest.raises(APIError) as exc_info:
            list(client.get_logs(sandbox_id=SANDBOX_ID, cmd_id=CMD_ID))

        assert exc_info.value.status_code == 500
        client.close()


class TestStreamingErrorsAsync:
    """Async get_logs raises APIError on non-2xx responses."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_logs_raises_on_401(self):
        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{SANDBOX_ID}/cmd/{CMD_ID}/logs").mock(
            return_value=httpx.Response(
                401,
                json={"error": {"code": "unauthorized", "message": "Authentication required."}},
            )
        )

        client = _async_client(host=SANDBOX_API_BASE, team_id="team_test", token="bad_token")
        with pytest.raises(APIError) as exc_info:
            async for _ in client.get_logs(sandbox_id=SANDBOX_ID, cmd_id=CMD_ID):
                pass

        assert exc_info.value.status_code == 401
        await client.aclose()

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_logs_raises_on_500(self):
        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{SANDBOX_ID}/cmd/{CMD_ID}/logs").mock(
            return_value=httpx.Response(
                500,
                json={"error": {"code": "internal_server_error", "message": "Something broke."}},
            )
        )

        client = _async_client(host=SANDBOX_API_BASE, team_id="team_test", token="tok")
        with pytest.raises(APIError) as exc_info:
            async for _ in client.get_logs(sandbox_id=SANDBOX_ID, cmd_id=CMD_ID):
                pass

        assert exc_info.value.status_code == 500
        await client.aclose()
