"""Integration tests for low-level sandbox network policy transport."""

from __future__ import annotations

import json

import httpx
import pytest
import respx

import vercel.sandbox  # noqa: F401

SANDBOX_API_BASE = "https://api.vercel.com"


def _sync_client(**kwargs):
    from vercel._internal.iter_coroutine import iter_coroutine
    from vercel._internal.sandbox.core import SyncSandboxOpsClient

    return iter_coroutine, SyncSandboxOpsClient(**kwargs)


def _async_client(**kwargs):
    from vercel._internal.sandbox.core import AsyncSandboxOpsClient

    return AsyncSandboxOpsClient(**kwargs)


class TestSandboxTransportCreateNetworkPolicy:
    @respx.mock
    def test_create_sandbox_sync_serializes_network_policy(
        self, mock_sandbox_create_response
    ) -> None:
        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": {
                        **mock_sandbox_create_response,
                        "networkPolicy": {"mode": "allow-all"},
                    },
                    "routes": [],
                },
            )
        )

        iter_coroutine, client = _sync_client(
            host=SANDBOX_API_BASE, team_id="team_test123", token="test_token"
        )
        response = iter_coroutine(
            client.create_sandbox(
                project_id="prj_test123",
                network_policy={"mode": "allow-all"},
            )
        )

        assert route.called
        body = json.loads(route.calls.last.request.content)
        assert body == {
            "projectId": "prj_test123",
            "networkPolicy": {"mode": "allow-all"},
        }
        assert response.sandbox.network_policy == "allow-all"

        client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_create_sandbox_async_serializes_network_policy(
        self, mock_sandbox_create_response
    ) -> None:
        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": {
                        **mock_sandbox_create_response,
                        "networkPolicy": {"mode": "allow-all"},
                    },
                    "routes": [],
                },
            )
        )

        client = _async_client(host=SANDBOX_API_BASE, team_id="team_test123", token="test_token")
        response = await client.create_sandbox(
            project_id="prj_test123",
            network_policy={"mode": "allow-all"},
        )

        assert route.called
        body = json.loads(route.calls.last.request.content)
        assert body == {
            "projectId": "prj_test123",
            "networkPolicy": {"mode": "allow-all"},
        }
        assert response.sandbox.network_policy == "allow-all"

        await client.aclose()


class TestSandboxTransportUpdateNetworkPolicy:
    @respx.mock
    def test_update_network_policy_sync(self, mock_sandbox_get_response) -> None:
        sandbox_id = "sbx_test123456"
        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/network-policy").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": {
                        **mock_sandbox_get_response,
                        "networkPolicy": {"mode": "deny-all"},
                    }
                },
            )
        )

        iter_coroutine, client = _sync_client(
            host=SANDBOX_API_BASE, team_id="team_test123", token="test_token"
        )
        response = iter_coroutine(
            client.update_network_policy(
                sandbox_id=sandbox_id,
                network_policy={"mode": "deny-all"},
            )
        )

        assert route.called
        body = json.loads(route.calls.last.request.content)
        assert body == {"networkPolicy": {"mode": "deny-all"}}
        assert response.sandbox.network_policy == "deny-all"

        client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_update_network_policy_async(self, mock_sandbox_get_response) -> None:
        sandbox_id = "sbx_test123456"
        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/network-policy").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": {
                        **mock_sandbox_get_response,
                        "networkPolicy": {"mode": "deny-all"},
                    }
                },
            )
        )

        client = _async_client(host=SANDBOX_API_BASE, team_id="team_test123", token="test_token")
        response = await client.update_network_policy(
            sandbox_id=sandbox_id,
            network_policy={"mode": "deny-all"},
        )

        assert route.called
        body = json.loads(route.calls.last.request.content)
        assert body == {"networkPolicy": {"mode": "deny-all"}}
        assert response.sandbox.network_policy == "deny-all"

        await client.aclose()
