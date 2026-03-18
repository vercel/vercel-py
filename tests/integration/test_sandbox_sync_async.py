"""Integration tests for Vercel Sandbox API using respx mocking.

Tests both sync and async variants (Sandbox and AsyncSandbox).
"""

import json
import tarfile
from io import BytesIO

import httpx
import pytest
import respx

from vercel.sandbox import (
    NetworkPolicyCustom,
    NetworkPolicyRule,
    NetworkPolicySubnets,
    NetworkTransformer,
)

# Base URL for Vercel Sandbox API
SANDBOX_API_BASE = "https://api.vercel.com"


class TestSandboxCreate:
    """Test sandbox creation operations."""

    @respx.mock
    def test_create_sandbox_sync(self, mock_env_clear, mock_sandbox_create_response):
        """Test synchronous sandbox creation."""
        from vercel.sandbox import Sandbox

        # Mock the sandbox creation endpoint
        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_create_response,
                    "routes": [
                        {
                            "port": 3000,
                            "subdomain": "test-sbx",
                            "url": "https://test-sbx.vercel.run",
                        }
                    ],
                },
            )
        )

        sandbox = Sandbox.create(
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        assert route.called
        assert sandbox.sandbox_id == "sbx_test123456"
        assert sandbox.status == "running"
        assert sandbox.timeout == 300

        # Cleanup
        sandbox.client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_create_sandbox_async(self, mock_env_clear, mock_sandbox_create_response):
        """Test asynchronous sandbox creation."""
        from vercel.sandbox import AsyncSandbox

        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_create_response,
                    "routes": [
                        {
                            "port": 3000,
                            "subdomain": "test-sbx",
                            "url": "https://test-sbx.vercel.run",
                        }
                    ],
                },
            )
        )

        sandbox = await AsyncSandbox.create(
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        assert route.called
        assert sandbox.sandbox_id == "sbx_test123456"
        assert sandbox.status == "running"

        # Cleanup
        await sandbox.client.aclose()

    @respx.mock
    def test_create_sandbox_with_options(self, mock_env_clear, mock_sandbox_create_response):
        """Test sandbox creation with all options."""
        from vercel.sandbox import Sandbox

        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_create_response,
                    "routes": [],
                },
            )
        )

        sandbox = Sandbox.create(
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
            ports=[3000, 8080],
            timeout=600000,
            runtime="nodejs20.x",
        )

        assert route.called
        # Verify request body
        import json

        body = json.loads(route.calls.last.request.content)
        assert body["ports"] == [3000, 8080]
        assert body["timeout"] == 600000
        assert body["runtime"] == "nodejs20.x"

        sandbox.client.close()

    @respx.mock
    def test_create_sandbox_with_env_sync(self, mock_env_clear, mock_sandbox_create_response):
        """Test sandbox creation with env dict."""
        from vercel.sandbox import Sandbox

        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_create_response,
                    "routes": [],
                },
            )
        )

        sandbox = Sandbox.create(
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
            env={"NODE_ENV": "production"},
        )

        assert route.called
        import json

        body = json.loads(route.calls.last.request.content)
        assert body["env"] == {"NODE_ENV": "production"}

        sandbox.client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_create_sandbox_with_env_async(
        self, mock_env_clear, mock_sandbox_create_response
    ):
        """Test async sandbox creation with env dict."""
        from vercel.sandbox import AsyncSandbox

        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_create_response,
                    "routes": [],
                },
            )
        )

        sandbox = await AsyncSandbox.create(
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
            env={"NODE_ENV": "production"},
        )

        assert route.called
        import json

        body = json.loads(route.calls.last.request.content)
        assert body["env"] == {"NODE_ENV": "production"}

        await sandbox.client.aclose()


class TestSandboxCreateNetworkPolicy:
    """Test sandbox creation request serialization for network policy."""

    @pytest.mark.parametrize(
        ("network_policy", "expected_policy"),
        [
            ("allow-all", {"mode": "allow-all"}),
            ("deny-all", {"mode": "deny-all"}),
            (
                NetworkPolicyCustom(
                    allow=["example.com", "*.example.net"],
                    subnets=NetworkPolicySubnets(
                        allow=["10.0.0.0/8"],
                        deny=["192.168.0.0/16"],
                    ),
                ),
                {
                    "mode": "custom",
                    "allowedDomains": ["example.com", "*.example.net"],
                    "allowedCIDRs": ["10.0.0.0/8"],
                    "deniedCIDRs": ["192.168.0.0/16"],
                },
            ),
            (
                NetworkPolicyCustom(
                    allow={
                        "example.com": [
                            NetworkPolicyRule(
                                transform=[NetworkTransformer(headers={"X-Trace": "trace-value"})]
                            )
                        ]
                    }
                ),
                {
                    "mode": "custom",
                    "allowedDomains": ["example.com"],
                    "injectionRules": [
                        {"domain": "example.com", "headers": {"X-Trace": "trace-value"}}
                    ],
                },
            ),
        ],
        ids=["allow_all", "deny_all", "list_form", "record_form"],
    )
    @respx.mock
    def test_create_sandbox_sync_serializes_network_policy(
        self, mock_env_clear, mock_sandbox_create_response, network_policy, expected_policy
    ):
        from vercel.sandbox import Sandbox

        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(
            return_value=httpx.Response(
                200,
                json={"sandbox": mock_sandbox_create_response, "routes": []},
            )
        )

        sandbox = Sandbox.create(
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
            ports=[3000, 8080],
            timeout=600000,
            runtime="nodejs20.x",
            network_policy=network_policy,
        )

        assert route.called
        body = json.loads(route.calls.last.request.content)
        assert body == {
            "projectId": "prj_test123",
            "ports": [3000, 8080],
            "timeout": 600000,
            "runtime": "nodejs20.x",
            "networkPolicy": expected_policy,
        }

        sandbox.client.close()

    @pytest.mark.parametrize(
        ("network_policy", "expected_policy"),
        [
            ("allow-all", {"mode": "allow-all"}),
            ("deny-all", {"mode": "deny-all"}),
            (
                NetworkPolicyCustom(
                    allow=["example.com", "*.example.net"],
                    subnets=NetworkPolicySubnets(
                        allow=["10.0.0.0/8"],
                        deny=["192.168.0.0/16"],
                    ),
                ),
                {
                    "mode": "custom",
                    "allowedDomains": ["example.com", "*.example.net"],
                    "allowedCIDRs": ["10.0.0.0/8"],
                    "deniedCIDRs": ["192.168.0.0/16"],
                },
            ),
            (
                NetworkPolicyCustom(
                    allow={
                        "example.com": [
                            NetworkPolicyRule(
                                transform=[NetworkTransformer(headers={"X-Trace": "trace-value"})]
                            )
                        ]
                    }
                ),
                {
                    "mode": "custom",
                    "allowedDomains": ["example.com"],
                    "injectionRules": [
                        {"domain": "example.com", "headers": {"X-Trace": "trace-value"}}
                    ],
                },
            ),
        ],
        ids=["allow_all", "deny_all", "list_form", "record_form"],
    )
    @respx.mock
    @pytest.mark.asyncio
    async def test_create_sandbox_async_serializes_network_policy(
        self, mock_env_clear, mock_sandbox_create_response, network_policy, expected_policy
    ):
        from vercel.sandbox import AsyncSandbox

        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(
            return_value=httpx.Response(
                200,
                json={"sandbox": mock_sandbox_create_response, "routes": []},
            )
        )

        sandbox = await AsyncSandbox.create(
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
            ports=[3000, 8080],
            timeout=600000,
            runtime="nodejs20.x",
            network_policy=network_policy,
        )

        assert route.called
        body = json.loads(route.calls.last.request.content)
        assert body == {
            "projectId": "prj_test123",
            "ports": [3000, 8080],
            "timeout": 600000,
            "runtime": "nodejs20.x",
            "networkPolicy": expected_policy,
        }

        await sandbox.client.aclose()

    @respx.mock
    def test_create_sandbox_sync_omits_network_policy_when_not_provided(
        self, mock_env_clear, mock_sandbox_create_response
    ):
        from vercel.sandbox import Sandbox

        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(
            return_value=httpx.Response(
                200,
                json={"sandbox": mock_sandbox_create_response, "routes": []},
            )
        )

        sandbox = Sandbox.create(
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        assert route.called
        body = json.loads(route.calls.last.request.content)
        assert "networkPolicy" not in body

        sandbox.client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_create_sandbox_async_omits_network_policy_when_not_provided(
        self, mock_env_clear, mock_sandbox_create_response
    ):
        from vercel.sandbox import AsyncSandbox

        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(
            return_value=httpx.Response(
                200,
                json={"sandbox": mock_sandbox_create_response, "routes": []},
            )
        )

        sandbox = await AsyncSandbox.create(
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        assert route.called
        body = json.loads(route.calls.last.request.content)
        assert "networkPolicy" not in body

        await sandbox.client.aclose()


class TestSandboxGet:
    """Test sandbox get operations."""

    @respx.mock
    def test_get_sandbox_sync(self, mock_env_clear, mock_sandbox_get_response):
        """Test synchronous get sandbox by ID."""
        from vercel.sandbox import Sandbox

        sandbox_id = "sbx_test123456"
        route = respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_get_response,
                    "routes": [],
                },
            )
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        assert route.called
        assert sandbox.sandbox_id == sandbox_id
        assert sandbox.status == "running"

        sandbox.client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_sandbox_async(self, mock_env_clear, mock_sandbox_get_response):
        """Test asynchronous get sandbox by ID."""
        from vercel.sandbox import AsyncSandbox

        sandbox_id = "sbx_test123456"
        route = respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_get_response,
                    "routes": [],
                },
            )
        )

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        assert route.called
        assert sandbox.sandbox_id == sandbox_id

        await sandbox.client.aclose()

    @respx.mock
    def test_get_sandbox_sync_exposes_mode_network_policy(
        self, mock_env_clear, mock_sandbox_get_response_with_mode_network_policy
    ):
        """Test synchronous get sandbox exposes a converted mode policy."""
        from vercel.sandbox import Sandbox

        sandbox_id = "sbx_test123456"
        route = respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_get_response_with_mode_network_policy,
                    "routes": [],
                },
            )
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        assert route.called
        assert sandbox.network_policy == "allow-all"

        sandbox.client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_sandbox_async_exposes_custom_network_policy(
        self, mock_env_clear, mock_sandbox_get_response_with_custom_network_policy
    ):
        """Test asynchronous get sandbox exposes a converted custom policy."""
        from vercel.sandbox import AsyncSandbox

        sandbox_id = "sbx_test123456"
        route = respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_get_response_with_custom_network_policy,
                    "routes": [],
                },
            )
        )

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        assert route.called
        assert sandbox.network_policy == NetworkPolicyCustom(
            allow={
                "example.com": [
                    NetworkPolicyRule(
                        transform=[NetworkTransformer(headers={"X-Trace": "<redacted>"})]
                    )
                ]
            }
        )

        await sandbox.client.aclose()


class TestSandboxUpdateNetworkPolicy:
    """Test sandbox network policy updates."""

    @pytest.mark.parametrize(
        ("network_policy", "expected_api_policy", "expected_public_policy"),
        [
            ("deny-all", {"mode": "deny-all"}, "deny-all"),
            (
                NetworkPolicyCustom(
                    allow=["example.com", "*.example.net"],
                    subnets=NetworkPolicySubnets(
                        allow=["10.0.0.0/8"],
                        deny=["192.168.0.0/16"],
                    ),
                ),
                {
                    "mode": "custom",
                    "allowedDomains": ["example.com", "*.example.net"],
                    "allowedCIDRs": ["10.0.0.0/8"],
                    "deniedCIDRs": ["192.168.0.0/16"],
                },
                NetworkPolicyCustom(
                    allow=["example.com", "*.example.net"],
                    subnets=NetworkPolicySubnets(
                        allow=["10.0.0.0/8"],
                        deny=["192.168.0.0/16"],
                    ),
                ),
            ),
            (
                NetworkPolicyCustom(
                    allow={
                        "example.com": [
                            NetworkPolicyRule(
                                transform=[NetworkTransformer(headers={"X-Trace": "trace-value"})]
                            )
                        ]
                    }
                ),
                {
                    "mode": "custom",
                    "allowedDomains": ["example.com"],
                    "injectionRules": [
                        {"domain": "example.com", "headers": {"X-Trace": "trace-value"}}
                    ],
                },
                NetworkPolicyCustom(
                    allow={
                        "example.com": [
                            NetworkPolicyRule(
                                transform=[NetworkTransformer(headers={"X-Trace": "<redacted>"})]
                            )
                        ]
                    }
                ),
            ),
        ],
        ids=["deny_all", "list_form", "record_form"],
    )
    @respx.mock
    def test_update_sandbox_sync_updates_network_policy(
        self,
        mock_env_clear,
        mock_sandbox_get_response,
        network_policy,
        expected_api_policy,
        expected_public_policy,
    ):
        from vercel.sandbox import Sandbox

        sandbox_id = "sbx_test123456"
        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_get_response,
                    "routes": [],
                },
            )
        )

        updated_response = dict(mock_sandbox_get_response)
        updated_response["networkPolicy"] = expected_api_policy
        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/network-policy").mock(
            return_value=httpx.Response(200, json={"sandbox": updated_response})
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        result = sandbox.update_network_policy(network_policy)

        assert route.called
        body = json.loads(route.calls.last.request.content)
        assert body == {"networkPolicy": expected_api_policy}
        assert result == expected_public_policy
        assert sandbox.network_policy == expected_public_policy

        sandbox.client.close()

    @respx.mock
    def test_update_sandbox_sync_raises_when_response_omits_network_policy(
        self, mock_env_clear, mock_sandbox_get_response
    ):
        from vercel.sandbox import Sandbox

        sandbox_id = "sbx_test123456"
        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_get_response,
                    "routes": [],
                },
            )
        )
        respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/network-policy").mock(
            return_value=httpx.Response(200, json={"sandbox": mock_sandbox_get_response})
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        with pytest.raises(RuntimeError, match="did not include network policy"):
            sandbox.update_network_policy("deny-all")

        sandbox.client.close()

    @pytest.mark.parametrize(
        ("network_policy", "expected_api_policy", "expected_public_policy"),
        [
            ("deny-all", {"mode": "deny-all"}, "deny-all"),
            (
                NetworkPolicyCustom(
                    allow=["example.com", "*.example.net"],
                    subnets=NetworkPolicySubnets(
                        allow=["10.0.0.0/8"],
                        deny=["192.168.0.0/16"],
                    ),
                ),
                {
                    "mode": "custom",
                    "allowedDomains": ["example.com", "*.example.net"],
                    "allowedCIDRs": ["10.0.0.0/8"],
                    "deniedCIDRs": ["192.168.0.0/16"],
                },
                NetworkPolicyCustom(
                    allow=["example.com", "*.example.net"],
                    subnets=NetworkPolicySubnets(
                        allow=["10.0.0.0/8"],
                        deny=["192.168.0.0/16"],
                    ),
                ),
            ),
            (
                NetworkPolicyCustom(
                    allow={
                        "example.com": [
                            NetworkPolicyRule(
                                transform=[NetworkTransformer(headers={"X-Trace": "trace-value"})]
                            )
                        ]
                    }
                ),
                {
                    "mode": "custom",
                    "allowedDomains": ["example.com"],
                    "injectionRules": [
                        {"domain": "example.com", "headers": {"X-Trace": "trace-value"}}
                    ],
                },
                NetworkPolicyCustom(
                    allow={
                        "example.com": [
                            NetworkPolicyRule(
                                transform=[NetworkTransformer(headers={"X-Trace": "<redacted>"})]
                            )
                        ]
                    }
                ),
            ),
        ],
        ids=["deny_all", "list_form", "record_form"],
    )
    @respx.mock
    @pytest.mark.asyncio
    async def test_update_sandbox_async_updates_network_policy(
        self,
        mock_env_clear,
        mock_sandbox_get_response,
        network_policy,
        expected_api_policy,
        expected_public_policy,
    ):
        from vercel.sandbox import AsyncSandbox

        sandbox_id = "sbx_test123456"
        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_get_response,
                    "routes": [],
                },
            )
        )

        updated_response = dict(mock_sandbox_get_response)
        updated_response["networkPolicy"] = expected_api_policy
        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/network-policy").mock(
            return_value=httpx.Response(200, json={"sandbox": updated_response})
        )

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        result = await sandbox.update_network_policy(network_policy)

        assert route.called
        body = json.loads(route.calls.last.request.content)
        assert body == {"networkPolicy": expected_api_policy}
        assert result == expected_public_policy
        assert sandbox.network_policy == expected_public_policy

        await sandbox.client.aclose()

    @respx.mock
    @pytest.mark.asyncio
    async def test_update_sandbox_async_raises_when_response_omits_network_policy(
        self, mock_env_clear, mock_sandbox_get_response
    ):
        from vercel.sandbox import AsyncSandbox

        sandbox_id = "sbx_test123456"
        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_get_response,
                    "routes": [],
                },
            )
        )
        respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/network-policy").mock(
            return_value=httpx.Response(200, json={"sandbox": mock_sandbox_get_response})
        )

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        with pytest.raises(RuntimeError, match="did not include network policy"):
            await sandbox.update_network_policy("deny-all")

        await sandbox.client.aclose()


class TestSandboxRunCommand:
    """Test sandbox command execution."""

    @respx.mock
    def test_run_command_sync(
        self, mock_env_clear, mock_sandbox_get_response, mock_sandbox_command_response
    ):
        """Test synchronous command execution."""
        from vercel.sandbox import Sandbox

        sandbox_id = "sbx_test123456"

        # Mock get sandbox
        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_get_response,
                    "routes": [],
                },
            )
        )

        # Mock run command
        cmd_id = mock_sandbox_command_response["commandId"]
        respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/cmd").mock(
            return_value=httpx.Response(
                200,
                json={
                    "command": {
                        "id": cmd_id,
                        "name": "echo",
                        "args": ["Hello, World!"],
                        "cwd": "/app",
                        "sandboxId": sandbox_id,
                        "exitCode": None,
                        "startedAt": 1705320600000,
                    }
                },
            )
        )

        # Mock wait for command (with query param wait=true)
        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/cmd/{cmd_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "command": {
                        "id": cmd_id,
                        "name": "echo",
                        "args": ["Hello, World!"],
                        "cwd": "/app",
                        "sandboxId": sandbox_id,
                        "exitCode": 0,
                        "startedAt": 1705320600000,
                        "stdout": "Hello, World!\n",
                        "stderr": "",
                    }
                },
            )
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        result = sandbox.run_command("echo", ["Hello, World!"])

        assert result.exit_code == 0
        # Note: stdout() is a method that fetches logs from API, not tested here

        sandbox.client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_run_command_async(
        self, mock_env_clear, mock_sandbox_get_response, mock_sandbox_command_response
    ):
        """Test asynchronous command execution."""
        from vercel.sandbox import AsyncSandbox

        sandbox_id = "sbx_test123456"
        cmd_id = mock_sandbox_command_response["commandId"]

        # Mock get sandbox
        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_get_response,
                    "routes": [],
                },
            )
        )

        # Mock run command
        respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/cmd").mock(
            return_value=httpx.Response(
                200,
                json={
                    "command": {
                        "id": cmd_id,
                        "name": "echo",
                        "args": ["Hello, World!"],
                        "cwd": "/app",
                        "sandboxId": sandbox_id,
                        "exitCode": None,
                        "startedAt": 1705320600000,
                    }
                },
            )
        )

        # Mock wait for command
        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/cmd/{cmd_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "command": {
                        "id": cmd_id,
                        "name": "echo",
                        "args": ["Hello, World!"],
                        "cwd": "/app",
                        "sandboxId": sandbox_id,
                        "exitCode": 0,
                        "startedAt": 1705320600000,
                        "stdout": "Hello, World!\n",
                        "stderr": "",
                    }
                },
            )
        )

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        result = await sandbox.run_command("echo", ["Hello, World!"])

        assert result.exit_code == 0

        await sandbox.client.aclose()


class TestSandboxFileOperations:
    """Test sandbox file operations."""

    @staticmethod
    def _extract_written_file(request: httpx.Request, name: str) -> tuple[tarfile.TarInfo, bytes]:
        with tarfile.open(fileobj=BytesIO(request.content), mode="r:gz") as tar:
            info = tar.getmember(name)
            extracted = tar.extractfile(info)
            assert extracted is not None
            return info, extracted.read()

    @respx.mock
    def test_write_files_sync_includes_mode(self, mock_env_clear, mock_sandbox_get_response):
        """Test synchronous file write preserves optional file mode in tarball."""
        from vercel.sandbox import Sandbox

        sandbox_id = "sbx_test123456"

        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_get_response,
                    "routes": [],
                },
            )
        )

        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/fs/write").mock(
            return_value=httpx.Response(200, json={})
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        sandbox.write_files(
            [
                {
                    "path": "bin/hello.sh",
                    "content": b"#!/bin/sh\necho hello\n",
                    "mode": 0o755,
                }
            ]
        )

        assert route.called
        request = route.calls.last.request
        assert request.headers["x-cwd"] == "/"

        info, data = self._extract_written_file(request, "app/bin/hello.sh")
        assert info.mode == 0o755
        assert data == b"#!/bin/sh\necho hello\n"

        sandbox.client.close()

    @respx.mock
    def test_write_files_sync_ignores_none_mode(self, mock_env_clear, mock_sandbox_get_response):
        """Test sync file write ignores mode=None instead of treating it as present."""
        from vercel.sandbox import Sandbox

        sandbox_id = "sbx_test123456"

        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_get_response,
                    "routes": [],
                },
            )
        )

        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/fs/write").mock(
            return_value=httpx.Response(200, json={})
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        sandbox.write_files([{"path": "tmp/file.txt", "content": b"hello", "mode": None}])

        assert route.called
        info, data = self._extract_written_file(route.calls.last.request, "app/tmp/file.txt")
        assert info.mode == tarfile.TarInfo("tmp/file.txt").mode
        assert data == b"hello"

        sandbox.client.close()

    @pytest.mark.parametrize("mode", [-1, 0o1000, 999999999])
    @respx.mock
    def test_write_files_sync_rejects_out_of_range_mode(
        self, mock_env_clear, mock_sandbox_get_response, mode: int
    ):
        """Test sync file write rejects invalid mode integers with a clear error."""
        from vercel.sandbox import Sandbox

        sandbox_id = "sbx_test123456"

        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_get_response,
                    "routes": [],
                },
            )
        )

        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/fs/write").mock(
            return_value=httpx.Response(200, json={})
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        with pytest.raises(ValueError, match="mode must be an integer between 0 and 0o777"):
            sandbox.write_files([{"path": "tmp/file.txt", "content": b"hello", "mode": mode}])

        assert not route.called

        sandbox.client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_write_files_async_includes_mode(self, mock_env_clear, mock_sandbox_get_response):
        """Test async file write preserves optional file mode in tarball."""
        from vercel.sandbox import AsyncSandbox

        sandbox_id = "sbx_test123456"

        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_get_response,
                    "routes": [],
                },
            )
        )

        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/fs/write").mock(
            return_value=httpx.Response(200, json={})
        )

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        await sandbox.write_files(
            [
                {
                    "path": "bin/hello.sh",
                    "content": b"#!/bin/sh\necho hello\n",
                    "mode": 0o755,
                }
            ]
        )

        assert route.called
        request = route.calls.last.request
        assert request.headers["x-cwd"] == "/"

        info, data = self._extract_written_file(request, "app/bin/hello.sh")
        assert info.mode == 0o755
        assert data == b"#!/bin/sh\necho hello\n"

        await sandbox.client.aclose()

    @respx.mock
    def test_read_file_sync(
        self, mock_env_clear, mock_sandbox_get_response, mock_sandbox_read_file_content
    ):
        """Test synchronous file read."""
        from vercel.sandbox import Sandbox

        sandbox_id = "sbx_test123456"

        # Mock get sandbox
        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_get_response,
                    "routes": [],
                },
            )
        )

        # Mock read file
        respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/fs/read").mock(
            return_value=httpx.Response(200, content=mock_sandbox_read_file_content)
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        content = sandbox.read_file("/etc/hosts")

        assert content is not None
        assert content == mock_sandbox_read_file_content

        sandbox.client.close()

    @respx.mock
    def test_read_file_not_found(self, mock_env_clear, mock_sandbox_get_response):
        """Test file read returns None for non-existent file."""
        from vercel.sandbox import Sandbox

        sandbox_id = "sbx_test123456"

        # Mock get sandbox
        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_get_response,
                    "routes": [],
                },
            )
        )

        # Mock read file - 404
        respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/fs/read").mock(
            return_value=httpx.Response(404, json={"error": {"code": "not_found"}})
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        content = sandbox.read_file("/nonexistent/file")

        assert content is None

        sandbox.client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_read_file_async(
        self, mock_env_clear, mock_sandbox_get_response, mock_sandbox_read_file_content
    ):
        """Test asynchronous file read."""
        from vercel.sandbox import AsyncSandbox

        sandbox_id = "sbx_test123456"

        # Mock get sandbox
        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_get_response,
                    "routes": [],
                },
            )
        )

        # Mock read file
        respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/fs/read").mock(
            return_value=httpx.Response(200, content=mock_sandbox_read_file_content)
        )

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        content = await sandbox.read_file("/etc/hosts")

        assert content is not None

        await sandbox.client.aclose()

    @respx.mock
    def test_mk_dir_sync(self, mock_env_clear, mock_sandbox_get_response):
        """Test synchronous directory creation."""
        from vercel.sandbox import Sandbox

        sandbox_id = "sbx_test123456"

        # Mock get sandbox
        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_get_response,
                    "routes": [],
                },
            )
        )

        # Mock mkdir
        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/fs/mkdir").mock(
            return_value=httpx.Response(200, json={})
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        sandbox.mk_dir("/app/data")

        assert route.called
        import json

        body = json.loads(route.calls.last.request.content)
        assert body["path"] == "/app/data"

        sandbox.client.close()


class TestSandboxStop:
    """Test sandbox stop operations."""

    @respx.mock
    def test_stop_sync(self, mock_env_clear, mock_sandbox_get_response):
        """Test synchronous sandbox stop."""
        from vercel.sandbox import Sandbox

        sandbox_id = "sbx_test123456"

        # Mock get sandbox
        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_get_response,
                    "routes": [],
                },
            )
        )

        # Mock stop
        stopped_response = dict(mock_sandbox_get_response)
        stopped_response["status"] = "stopped"
        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/stop").mock(
            return_value=httpx.Response(200, json={"sandbox": stopped_response})
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        sandbox.stop()

        assert route.called

        sandbox.client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_stop_async(self, mock_env_clear, mock_sandbox_get_response):
        """Test asynchronous sandbox stop."""
        from vercel.sandbox import AsyncSandbox

        sandbox_id = "sbx_test123456"

        # Mock get sandbox
        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_get_response,
                    "routes": [],
                },
            )
        )

        # Mock stop
        stopped_response = dict(mock_sandbox_get_response)
        stopped_response["status"] = "stopped"
        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/stop").mock(
            return_value=httpx.Response(200, json={"sandbox": stopped_response})
        )

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        await sandbox.stop()

        assert route.called

        await sandbox.client.aclose()


class TestSandboxContextManager:
    """Test sandbox context manager behavior."""

    @respx.mock
    def test_context_manager_sync(self, mock_env_clear, mock_sandbox_create_response):
        """Test sync context manager stops sandbox on exit."""
        from vercel.sandbox import Sandbox

        sandbox_id = "sbx_test123456"

        # Mock create
        respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_create_response,
                    "routes": [],
                },
            )
        )

        # Mock stop
        stopped_response = dict(mock_sandbox_create_response)
        stopped_response["status"] = "stopped"
        stop_route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/stop").mock(
            return_value=httpx.Response(200, json={"sandbox": stopped_response})
        )

        with Sandbox.create(
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        ) as sandbox:
            assert sandbox.status == "running"

        # Stop should have been called
        assert stop_route.called

    @respx.mock
    @pytest.mark.asyncio
    async def test_context_manager_async(self, mock_env_clear, mock_sandbox_create_response):
        """Test async context manager stops sandbox on exit."""
        from vercel.sandbox import AsyncSandbox

        sandbox_id = "sbx_test123456"

        # Mock create
        respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_create_response,
                    "routes": [],
                },
            )
        )

        # Mock stop
        stopped_response = dict(mock_sandbox_create_response)
        stopped_response["status"] = "stopped"
        stop_route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/stop").mock(
            return_value=httpx.Response(200, json={"sandbox": stopped_response})
        )

        async with await AsyncSandbox.create(
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        ) as sandbox:
            assert sandbox.status == "running"

        # Stop should have been called
        assert stop_route.called


class TestSandboxSnapshot:
    """Test sandbox snapshot operations."""

    @respx.mock
    def test_create_snapshot_sync(
        self, mock_env_clear, mock_sandbox_get_response, mock_sandbox_snapshot_response
    ):
        """Test synchronous snapshot creation."""
        from vercel.sandbox import Sandbox

        sandbox_id = "sbx_test123456"

        # Mock get sandbox
        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_get_response,
                    "routes": [],
                },
            )
        )

        # Mock create snapshot
        stopped_response = dict(mock_sandbox_get_response)
        stopped_response["status"] = "stopped"
        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/snapshot").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": stopped_response,
                    "snapshot": mock_sandbox_snapshot_response,
                },
            )
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        snapshot = sandbox.snapshot()

        assert route.called
        assert snapshot.snapshot_id == mock_sandbox_snapshot_response["id"]
        # Sandbox should be stopped after snapshot
        assert sandbox.status == "stopped"

        sandbox.client.close()


class TestSandboxExtendTimeout:
    """Test sandbox timeout extension."""

    @respx.mock
    def test_extend_timeout_sync(self, mock_env_clear, mock_sandbox_get_response):
        """Test synchronous timeout extension."""
        from vercel.sandbox import Sandbox

        sandbox_id = "sbx_test123456"

        # Mock get sandbox
        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_get_response,
                    "routes": [],
                },
            )
        )

        # Mock extend timeout
        extended_response = dict(mock_sandbox_get_response)
        extended_response["timeout"] = 600
        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/extend-timeout").mock(
            return_value=httpx.Response(200, json={"sandbox": extended_response})
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        sandbox.extend_timeout(300000)  # 5 minutes

        assert route.called
        import json

        body = json.loads(route.calls.last.request.content)
        assert body["duration"] == 300000
        assert sandbox.timeout == 600

        sandbox.client.close()


class TestSandboxDomain:
    """Test sandbox domain resolution."""

    @respx.mock
    def test_domain_resolution(self, mock_env_clear, mock_sandbox_get_response):
        """Test domain resolution for ports."""
        from vercel.sandbox import Sandbox

        sandbox_id = "sbx_test123456"

        # Mock get sandbox with routes
        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_get_response,
                    "routes": [
                        {
                            "port": 3000,
                            "subdomain": "app-3000",
                            "url": "https://app-3000.vercel.run",
                        },
                        {
                            "port": 8080,
                            "subdomain": "api-8080",
                            "url": "https://api-8080.vercel.run",
                        },
                    ],
                },
            )
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        # Test URL resolution
        assert sandbox.domain(3000) == "https://app-3000.vercel.run"
        assert sandbox.domain(8080) == "https://api-8080.vercel.run"

        # Test invalid port
        with pytest.raises(ValueError, match="No route for port"):
            sandbox.domain(9999)

        sandbox.client.close()


class TestSandboxRefresh:
    """Test sandbox refresh operations."""

    @respx.mock
    def test_refresh_sync(self, mock_env_clear, mock_sandbox_get_response):
        """Test synchronous sandbox refresh updates state in place."""
        from vercel.sandbox import Sandbox

        sandbox_id = "sbx_test123456"

        # Mock get sandbox (initial fetch)
        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            side_effect=[
                httpx.Response(
                    200,
                    json={
                        "sandbox": mock_sandbox_get_response,
                        "routes": [],
                    },
                ),
                # Second call (refresh) returns updated status
                httpx.Response(
                    200,
                    json={
                        "sandbox": {**mock_sandbox_get_response, "status": "stopped"},
                        "routes": [
                            {
                                "port": 3000,
                                "subdomain": "new-route",
                                "url": "https://new-route.vercel.run",
                            }
                        ],
                    },
                ),
            ]
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        assert sandbox.status == "running"
        assert sandbox.routes == []

        sandbox.refresh()

        assert sandbox.status == "stopped"
        assert len(sandbox.routes) == 1
        assert sandbox.routes[0]["port"] == 3000

        sandbox.client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_refresh_async(self, mock_env_clear, mock_sandbox_get_response):
        """Test asynchronous sandbox refresh updates state in place."""
        from vercel.sandbox import AsyncSandbox

        sandbox_id = "sbx_test123456"

        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            side_effect=[
                httpx.Response(
                    200,
                    json={
                        "sandbox": mock_sandbox_get_response,
                        "routes": [],
                    },
                ),
                httpx.Response(
                    200,
                    json={
                        "sandbox": {**mock_sandbox_get_response, "status": "stopped"},
                        "routes": [],
                    },
                ),
            ]
        )

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        assert sandbox.status == "running"

        await sandbox.refresh()

        assert sandbox.status == "stopped"

        await sandbox.client.aclose()


class TestSandboxWaitForStatus:
    """Test sandbox wait_for_status operations."""

    @respx.mock
    def test_wait_for_status_already_matched(self, mock_env_clear, mock_sandbox_get_response):
        """Test wait_for_status returns immediately if already at target status."""
        from vercel.sandbox import Sandbox

        sandbox_id = "sbx_test123456"

        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_get_response,
                    "routes": [],
                },
            )
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        assert sandbox.status == "running"
        # Should return immediately without making additional API calls
        sandbox.wait_for_status("running")

        sandbox.client.close()

    @respx.mock
    def test_wait_for_status_polls(self, mock_env_clear, mock_sandbox_get_response):
        """Test wait_for_status polls until status matches."""
        from vercel.sandbox import Sandbox

        sandbox_id = "sbx_test123456"
        pending_response = {**mock_sandbox_get_response, "status": "pending"}

        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            side_effect=[
                # Initial get returns pending
                httpx.Response(
                    200,
                    json={"sandbox": pending_response, "routes": []},
                ),
                # First refresh: still pending
                httpx.Response(
                    200,
                    json={"sandbox": pending_response, "routes": []},
                ),
                # Second refresh: now running
                httpx.Response(
                    200,
                    json={"sandbox": mock_sandbox_get_response, "routes": []},
                ),
            ]
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        assert sandbox.status == "pending"
        sandbox.wait_for_status("running", poll_interval=0.01)
        assert sandbox.status == "running"

        sandbox.client.close()

    @respx.mock
    def test_wait_for_status_timeout(self, mock_env_clear, mock_sandbox_get_response):
        """Test wait_for_status raises TimeoutError."""
        from vercel.sandbox import Sandbox

        sandbox_id = "sbx_test123456"
        pending_response = {**mock_sandbox_get_response, "status": "pending"}

        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={"sandbox": pending_response, "routes": []},
            )
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        with pytest.raises(TimeoutError, match="did not reach 'running' status"):
            sandbox.wait_for_status("running", timeout=0.05, poll_interval=0.01)

        sandbox.client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_wait_for_status_async_polls(self, mock_env_clear, mock_sandbox_get_response):
        """Test async wait_for_status polls until status matches."""
        from vercel.sandbox import AsyncSandbox

        sandbox_id = "sbx_test123456"
        pending_response = {**mock_sandbox_get_response, "status": "pending"}

        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            side_effect=[
                httpx.Response(
                    200,
                    json={"sandbox": pending_response, "routes": []},
                ),
                httpx.Response(
                    200,
                    json={"sandbox": pending_response, "routes": []},
                ),
                httpx.Response(
                    200,
                    json={"sandbox": mock_sandbox_get_response, "routes": []},
                ),
            ]
        )

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        assert sandbox.status == "pending"
        await sandbox.wait_for_status("running", poll_interval=0.01)
        assert sandbox.status == "running"

        await sandbox.client.aclose()

    @respx.mock
    @pytest.mark.asyncio
    async def test_wait_for_status_async_timeout(self, mock_env_clear, mock_sandbox_get_response):
        """Test async wait_for_status raises TimeoutError."""
        from vercel.sandbox import AsyncSandbox

        sandbox_id = "sbx_test123456"
        pending_response = {**mock_sandbox_get_response, "status": "pending"}

        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={"sandbox": pending_response, "routes": []},
            )
        )

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        with pytest.raises(TimeoutError, match="did not reach 'running' status"):
            await sandbox.wait_for_status("running", timeout=0.05, poll_interval=0.01)

        await sandbox.client.aclose()
