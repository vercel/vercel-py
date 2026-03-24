"""Integration tests for Vercel Sandbox API using respx mocking.

Tests both sync and async variants (Sandbox and AsyncSandbox).
"""

import json
import tarfile
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest
import respx

from vercel.sandbox import (
    NetworkPolicyCustom,
    NetworkPolicyRule,
    NetworkPolicySubnets,
    NetworkTransformer,
    SandboxNotFoundError,
    SandboxServerError,
)

# Base URL for Vercel Sandbox API
SANDBOX_API_BASE = "https://api.vercel.com"


def _sandbox_with_id(
    base_sandbox: dict,
    sandbox_id: str,
    *,
    created_at: int,
) -> dict:
    sandbox = dict(base_sandbox)
    sandbox["id"] = sandbox_id
    sandbox["createdAt"] = created_at
    sandbox["requestedAt"] = created_at
    sandbox["updatedAt"] = created_at
    sandbox["startedAt"] = created_at + 1000
    return sandbox


def _snapshot_with_id(
    base_snapshot: dict,
    snapshot_id: str,
    *,
    created_at: int,
) -> dict:
    snapshot = dict(base_snapshot)
    snapshot["id"] = snapshot_id
    snapshot["createdAt"] = created_at
    snapshot["updatedAt"] = created_at
    return snapshot


async def _collect_async_pages(page) -> list:
    return [current_page async for current_page in page.iter_pages()]


async def _collect_async_items(page) -> list:
    return [sandbox async for sandbox in page.iter_items()]


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


class TestSandboxList:
    """Test sandbox listing pagination behavior."""

    @respx.mock
    def test_list_sandbox_sync_serializes_datetime_filters_and_typed_pages(
        self, mock_env_clear, mock_sandbox_get_response
    ):
        from vercel._internal.sandbox.models import Sandbox as SandboxModel
        from vercel.sandbox import Sandbox

        project = "sandbox-project"
        limit = 2
        since = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)
        until = datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)
        expected_since = str(int(since.timestamp() * 1000))
        expected_until = str(int(until.timestamp() * 1000))
        next_until = "1705310400000"

        first_page = {
            "sandboxes": [
                _sandbox_with_id(
                    mock_sandbox_get_response,
                    "sbx_filtered_1",
                    created_at=1705311000000,
                ),
                _sandbox_with_id(
                    mock_sandbox_get_response,
                    "sbx_filtered_2",
                    created_at=1705310700000,
                ),
            ],
            "pagination": {
                "count": 3,
                "next": int(next_until),
                "prev": None,
            },
        }
        second_page = {
            "sandboxes": [
                _sandbox_with_id(
                    mock_sandbox_get_response,
                    "sbx_filtered_3",
                    created_at=1705310400000,
                ),
            ],
            "pagination": {
                "count": 3,
                "next": None,
                "prev": 1705311000000,
            },
        }
        requests: list[dict[str, str]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            params = dict(request.url.params)
            requests.append(params)
            if params.get("until") == next_until:
                return httpx.Response(200, json=second_page)
            return httpx.Response(200, json=first_page)

        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(side_effect=handler)

        page = Sandbox.list(
            token="test_token",
            team_id="team_test123",
            project_id=project,
            limit=limit,
            since=since,
            until=until,
        )

        assert requests == [
            {
                "teamId": "team_test123",
                "project": project,
                "limit": str(limit),
                "since": expected_since,
                "until": expected_until,
            }
        ]
        assert isinstance(page.sandboxes[0], SandboxModel)
        assert page.sandboxes[0].id == "sbx_filtered_1"
        assert page.sandboxes[0].created_at == 1705311000000
        assert page.sandboxes[0].requested_at == 1705311000000
        assert page.pagination.count == 3
        assert page.next_page_info() is not None
        assert page.next_page_info().until == int(next_until)

        assert [sandbox.id for sandbox in page.iter_items()] == [
            "sbx_filtered_1",
            "sbx_filtered_2",
            "sbx_filtered_3",
        ]
        assert requests == [
            {
                "teamId": "team_test123",
                "project": project,
                "limit": str(limit),
                "since": expected_since,
                "until": expected_until,
            },
            {
                "teamId": "team_test123",
                "project": project,
                "limit": str(limit),
                "since": expected_since,
                "until": next_until,
            },
        ]

    @respx.mock
    @pytest.mark.asyncio
    async def test_list_sandbox_async_serializes_integer_filters_and_typed_pages(
        self, mock_env_clear, mock_sandbox_get_response
    ):
        from vercel._internal.sandbox.models import Sandbox as SandboxModel
        from vercel.sandbox import AsyncSandbox

        project = "sandbox-project"
        limit = 2
        since = 1705312800000
        until = 1705314600000
        next_until = "1705312500000"

        first_page = {
            "sandboxes": [
                _sandbox_with_id(
                    mock_sandbox_get_response,
                    "sbx_async_filtered_1",
                    created_at=1705313400000,
                ),
                _sandbox_with_id(
                    mock_sandbox_get_response,
                    "sbx_async_filtered_2",
                    created_at=1705313100000,
                ),
            ],
            "pagination": {
                "count": 3,
                "next": int(next_until),
                "prev": None,
            },
        }
        second_page = {
            "sandboxes": [
                _sandbox_with_id(
                    mock_sandbox_get_response,
                    "sbx_async_filtered_3",
                    created_at=1705312500000,
                ),
            ],
            "pagination": {
                "count": 3,
                "next": None,
                "prev": 1705313400000,
            },
        }
        requests: list[dict[str, str]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            params = dict(request.url.params)
            requests.append(params)
            if params.get("until") == next_until:
                return httpx.Response(200, json=second_page)
            return httpx.Response(200, json=first_page)

        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(side_effect=handler)

        page = await AsyncSandbox.list(
            token="test_token",
            team_id="team_test123",
            project_id=project,
            limit=limit,
            since=since,
            until=until,
        )

        assert requests == [
            {
                "teamId": "team_test123",
                "project": project,
                "limit": str(limit),
                "since": str(since),
                "until": str(until),
            }
        ]
        assert isinstance(page.sandboxes[0], SandboxModel)
        assert page.sandboxes[0].id == "sbx_async_filtered_1"
        assert page.sandboxes[0].created_at == 1705313400000
        assert page.sandboxes[0].requested_at == 1705313400000
        assert page.pagination.count == 3
        assert page.next_page_info() is not None
        assert page.next_page_info().until == int(next_until)

        items = await _collect_async_items(page)
        assert [sandbox.id for sandbox in items] == [
            "sbx_async_filtered_1",
            "sbx_async_filtered_2",
            "sbx_async_filtered_3",
        ]
        assert requests == [
            {
                "teamId": "team_test123",
                "project": project,
                "limit": str(limit),
                "since": str(since),
                "until": str(until),
            },
            {
                "teamId": "team_test123",
                "project": project,
                "limit": str(limit),
                "since": str(since),
                "until": next_until,
            },
        ]

    @respx.mock
    def test_list_sandbox_sync_iterates_pages_and_items(
        self, mock_env_clear, mock_sandbox_get_response
    ):
        from vercel.sandbox import Sandbox

        first_page = {
            "sandboxes": [
                _sandbox_with_id(
                    mock_sandbox_get_response,
                    "sbx_page_1",
                    created_at=1705320600000,
                ),
                _sandbox_with_id(
                    mock_sandbox_get_response,
                    "sbx_page_2",
                    created_at=1705320000000,
                ),
            ],
            "pagination": {
                "count": 3,
                "next": 1705319400000,
                "prev": None,
            },
        }
        second_page = {
            "sandboxes": [
                _sandbox_with_id(
                    mock_sandbox_get_response,
                    "sbx_page_3",
                    created_at=1705319400000,
                ),
            ],
            "pagination": {
                "count": 3,
                "next": None,
                "prev": 1705320600000,
            },
        }
        requests: list[dict[str, str]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            params = dict(request.url.params)
            requests.append(params)
            if params.get("until") == "1705319400000":
                return httpx.Response(200, json=second_page)
            return httpx.Response(200, json=first_page)

        route = respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(side_effect=handler)

        page = Sandbox.list(
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        assert route.called
        assert [sandbox.id for sandbox in page.sandboxes] == ["sbx_page_1", "sbx_page_2"]
        assert page.pagination.count == 3
        assert page.pagination.next == 1705319400000
        assert page.has_next_page() is True
        assert page.next_page_info() is not None
        assert page.next_page_info().until == 1705319400000

        pages = list(page.iter_pages())
        assert [[sandbox.id for sandbox in current_page.sandboxes] for current_page in pages] == [
            ["sbx_page_1", "sbx_page_2"],
            ["sbx_page_3"],
        ]
        assert requests == [
            {"teamId": "team_test123", "project": "prj_test123"},
            {"teamId": "team_test123", "project": "prj_test123", "until": "1705319400000"},
        ]

        requests.clear()
        items_page = Sandbox.list(
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )
        assert [sandbox.id for sandbox in items_page.iter_items()] == [
            "sbx_page_1",
            "sbx_page_2",
            "sbx_page_3",
        ]
        assert requests == [
            {"teamId": "team_test123", "project": "prj_test123"},
            {"teamId": "team_test123", "project": "prj_test123", "until": "1705319400000"},
        ]

    @respx.mock
    @pytest.mark.asyncio
    async def test_list_sandbox_async_iterates_pages_and_items(
        self, mock_env_clear, mock_sandbox_get_response
    ):
        from vercel.sandbox import AsyncSandbox

        first_page = {
            "sandboxes": [
                _sandbox_with_id(
                    mock_sandbox_get_response,
                    "sbx_async_1",
                    created_at=1705320600000,
                ),
                _sandbox_with_id(
                    mock_sandbox_get_response,
                    "sbx_async_2",
                    created_at=1705320000000,
                ),
            ],
            "pagination": {
                "count": 3,
                "next": 1705319400000,
                "prev": None,
            },
        }
        second_page = {
            "sandboxes": [
                _sandbox_with_id(
                    mock_sandbox_get_response,
                    "sbx_async_3",
                    created_at=1705319400000,
                ),
            ],
            "pagination": {
                "count": 3,
                "next": None,
                "prev": 1705320600000,
            },
        }
        requests: list[dict[str, str]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            params = dict(request.url.params)
            requests.append(params)
            if params.get("until") == "1705319400000":
                return httpx.Response(200, json=second_page)
            return httpx.Response(200, json=first_page)

        route = respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(side_effect=handler)

        page = await AsyncSandbox.list(
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        assert route.called
        assert [sandbox.id for sandbox in page.sandboxes] == ["sbx_async_1", "sbx_async_2"]
        assert page.pagination.count == 3
        assert page.pagination.next == 1705319400000
        assert page.has_next_page() is True
        assert page.next_page_info() is not None
        assert page.next_page_info().until == 1705319400000
        assert requests == [{"teamId": "team_test123", "project": "prj_test123"}]

        requests.clear()
        next_page = await page.get_next_page()
        assert next_page is not None
        assert [sandbox.id for sandbox in next_page.sandboxes] == ["sbx_async_3"]
        assert requests == [
            {"teamId": "team_test123", "project": "prj_test123", "until": "1705319400000"}
        ]

        requests.clear()
        pages = await _collect_async_pages(page)
        assert [[sandbox.id for sandbox in current_page.sandboxes] for current_page in pages] == [
            ["sbx_async_1", "sbx_async_2"],
            ["sbx_async_3"],
        ]
        assert requests == [
            {"teamId": "team_test123", "project": "prj_test123", "until": "1705319400000"}
        ]

        requests.clear()
        items_page = await AsyncSandbox.list(
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )
        items = await _collect_async_items(items_page)
        assert [sandbox.id for sandbox in items] == [
            "sbx_async_1",
            "sbx_async_2",
            "sbx_async_3",
        ]
        assert requests == [
            {"teamId": "team_test123", "project": "prj_test123"},
            {"teamId": "team_test123", "project": "prj_test123", "until": "1705319400000"},
        ]

    @respx.mock
    @pytest.mark.asyncio
    async def test_list_sandbox_async_builder_supports_direct_iteration(
        self, mock_env_clear, mock_sandbox_get_response
    ):
        from vercel.sandbox import AsyncSandbox

        first_page = {
            "sandboxes": [
                _sandbox_with_id(
                    mock_sandbox_get_response,
                    "sbx_builder_1",
                    created_at=1705320600000,
                ),
                _sandbox_with_id(
                    mock_sandbox_get_response,
                    "sbx_builder_2",
                    created_at=1705320000000,
                ),
            ],
            "pagination": {
                "count": 3,
                "next": 1705319400000,
                "prev": None,
            },
        }
        second_page = {
            "sandboxes": [
                _sandbox_with_id(
                    mock_sandbox_get_response,
                    "sbx_builder_3",
                    created_at=1705319400000,
                ),
            ],
            "pagination": {
                "count": 3,
                "next": None,
                "prev": 1705320600000,
            },
        }
        requests: list[dict[str, str]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            params = dict(request.url.params)
            requests.append(params)
            if params.get("until") == "1705319400000":
                return httpx.Response(200, json=second_page)
            return httpx.Response(200, json=first_page)

        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(side_effect=handler)

        item_ids = [
            sandbox.id
            async for sandbox in AsyncSandbox.list(
                token="test_token",
                team_id="team_test123",
                project_id="prj_test123",
            )
        ]
        assert item_ids == ["sbx_builder_1", "sbx_builder_2", "sbx_builder_3"]
        assert requests == [
            {"teamId": "team_test123", "project": "prj_test123"},
            {"teamId": "team_test123", "project": "prj_test123", "until": "1705319400000"},
        ]

        requests.clear()
        pages = await _collect_async_pages(
            AsyncSandbox.list(
                token="test_token",
                team_id="team_test123",
                project_id="prj_test123",
            )
        )
        assert [[sandbox.id for sandbox in current_page.sandboxes] for current_page in pages] == [
            ["sbx_builder_1", "sbx_builder_2"],
            ["sbx_builder_3"],
        ]
        assert requests == [
            {"teamId": "team_test123", "project": "prj_test123"},
            {"teamId": "team_test123", "project": "prj_test123", "until": "1705319400000"},
        ]

    @respx.mock
    def test_list_sandbox_sync_single_page_does_not_fetch_more(
        self, mock_env_clear, mock_sandbox_get_response
    ):
        from vercel.sandbox import Sandbox

        response = {
            "sandboxes": [
                _sandbox_with_id(
                    mock_sandbox_get_response,
                    "sbx_single_page",
                    created_at=1705320600000,
                ),
            ],
            "pagination": {
                "count": 1,
                "next": None,
                "prev": None,
            },
        }
        requests: list[dict[str, str]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(dict(request.url.params))
            return httpx.Response(200, json=response)

        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(side_effect=handler)

        page = Sandbox.list(
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        assert page.has_next_page() is False
        assert page.next_page_info() is None
        assert page.get_next_page() is None
        assert [
            [sandbox.id for sandbox in current_page.sandboxes] for current_page in page.iter_pages()
        ] == [
            ["sbx_single_page"],
        ]
        assert [sandbox.id for sandbox in page.iter_items()] == ["sbx_single_page"]
        assert requests == [{"teamId": "team_test123", "project": "prj_test123"}]

    @respx.mock
    @pytest.mark.asyncio
    async def test_list_sandbox_async_terminal_page_does_not_fetch_more(
        self, mock_env_clear, mock_sandbox_get_response
    ):
        from vercel.sandbox import AsyncSandbox

        response = {
            "sandboxes": [
                _sandbox_with_id(
                    mock_sandbox_get_response,
                    "sbx_async_terminal",
                    created_at=1705320600000,
                ),
            ],
            "pagination": {
                "count": 1,
                "next": None,
                "prev": None,
            },
        }
        requests: list[dict[str, str]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(dict(request.url.params))
            return httpx.Response(200, json=response)

        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes").mock(side_effect=handler)

        page = await AsyncSandbox.list(
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        assert page.has_next_page() is False
        assert page.next_page_info() is None
        assert await page.get_next_page() is None
        pages = await _collect_async_pages(page)
        assert [[sandbox.id for sandbox in current_page.sandboxes] for current_page in pages] == [
            ["sbx_async_terminal"],
        ]
        items = await _collect_async_items(page)
        assert [sandbox.id for sandbox in items] == ["sbx_async_terminal"]
        assert requests == [{"teamId": "team_test123", "project": "prj_test123"}]


class TestSnapshotList:
    """Test snapshot listing operations."""

    @respx.mock
    def test_list_snapshot_sync_serializes_filters_and_iterates_pages(
        self, mock_env_clear, mock_sandbox_snapshot_response
    ):
        from vercel.sandbox import Snapshot

        project = "snapshot-project"
        limit = 2
        since = datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc)
        until = datetime(2024, 1, 15, 12, 30, tzinfo=timezone.utc)
        expected_since = str(int(since.timestamp() * 1000))
        expected_until = str(int(until.timestamp() * 1000))
        next_until = "1705320000000"

        first_page = {
            "snapshots": [
                _snapshot_with_id(
                    mock_sandbox_snapshot_response,
                    "snap_list_1",
                    created_at=1705320600000,
                ),
                _snapshot_with_id(
                    mock_sandbox_snapshot_response,
                    "snap_list_2",
                    created_at=1705320300000,
                ),
            ],
            "pagination": {
                "count": 3,
                "next": int(next_until),
                "prev": None,
            },
        }
        second_page = {
            "snapshots": [
                _snapshot_with_id(
                    mock_sandbox_snapshot_response,
                    "snap_list_3",
                    created_at=1705320000000,
                ),
            ],
            "pagination": {
                "count": 3,
                "next": None,
                "prev": 1705320600000,
            },
        }
        requests: list[dict[str, str]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            params = dict(request.url.params)
            requests.append(params)
            if params.get("until") == next_until:
                return httpx.Response(200, json=second_page)
            return httpx.Response(200, json=first_page)

        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/snapshots").mock(side_effect=handler)

        page = Snapshot.list(
            token="test_token",
            team_id="team_test123",
            project_id=project,
            limit=limit,
            since=since,
            until=until,
        )

        assert requests == [
            {
                "teamId": "team_test123",
                "project": project,
                "limit": str(limit),
                "since": expected_since,
                "until": expected_until,
            }
        ]
        assert [
            (snapshot.id, snapshot.created_at, snapshot.expires_at) for snapshot in page.snapshots
        ] == [
            (
                "snap_list_1",
                1705320600000,
                mock_sandbox_snapshot_response["expiresAt"],
            ),
            (
                "snap_list_2",
                1705320300000,
                mock_sandbox_snapshot_response["expiresAt"],
            ),
        ]
        assert page.pagination.count == 3
        assert page.next_page_info() is not None
        assert page.next_page_info().until == int(next_until)

    @respx.mock
    @pytest.mark.asyncio
    async def test_list_snapshot_async_serializes_integer_filters_and_iterates_pages(
        self, mock_env_clear, mock_sandbox_snapshot_response
    ):
        from vercel.sandbox import AsyncSnapshot

        project = "snapshot-project"
        limit = 2
        since = 1705321200000
        until = 1705323000000
        next_until = "1705319400000"

        first_page = {
            "snapshots": [
                _snapshot_with_id(
                    mock_sandbox_snapshot_response,
                    "snap_async_1",
                    created_at=1705320600000,
                ),
                _snapshot_with_id(
                    mock_sandbox_snapshot_response,
                    "snap_async_2",
                    created_at=1705320000000,
                ),
            ],
            "pagination": {
                "count": 3,
                "next": int(next_until),
                "prev": None,
            },
        }
        second_page = {
            "snapshots": [
                _snapshot_with_id(
                    mock_sandbox_snapshot_response,
                    "snap_async_3",
                    created_at=1705319400000,
                ),
            ],
            "pagination": {
                "count": 3,
                "next": None,
                "prev": 1705320600000,
            },
        }
        requests: list[dict[str, str]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            params = dict(request.url.params)
            requests.append(params)
            if params.get("until") == next_until:
                return httpx.Response(200, json=second_page)
            return httpx.Response(200, json=first_page)

        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/snapshots").mock(side_effect=handler)

        page = await AsyncSnapshot.list(
            token="test_token",
            team_id="team_test123",
            project_id=project,
            limit=limit,
            since=since,
            until=until,
        )

        assert requests == [
            {
                "teamId": "team_test123",
                "project": project,
                "limit": str(limit),
                "since": str(since),
                "until": str(until),
            }
        ]
        assert [
            (snapshot.id, snapshot.created_at, snapshot.expires_at) for snapshot in page.snapshots
        ] == [
            (
                "snap_async_1",
                1705320600000,
                mock_sandbox_snapshot_response["expiresAt"],
            ),
            (
                "snap_async_2",
                1705320000000,
                mock_sandbox_snapshot_response["expiresAt"],
            ),
        ]
        assert page.pagination.count == 3
        assert page.next_page_info() is not None
        assert page.next_page_info().until == int(next_until)

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
        assert body == expected_api_policy
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
        assert body == expected_api_policy
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
    def test_iter_file_sync(
        self, mock_env_clear, mock_sandbox_get_response, mock_sandbox_read_file_content
    ):
        """Test synchronous streamed file read."""
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

        respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/fs/read").mock(
            return_value=httpx.Response(200, content=mock_sandbox_read_file_content)
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        stream = sandbox.iter_file("/etc/hosts", chunk_size=4)

        assert stream is not None
        assert b"".join(stream) == mock_sandbox_read_file_content

        sandbox.client.close()

    @respx.mock
    def test_read_file_not_found(self, mock_env_clear, mock_sandbox_get_response):
        """Test file read raises for non-existent file."""
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

        with pytest.raises(SandboxNotFoundError, match="HTTP 404"):
            sandbox.read_file("/nonexistent/file")

        sandbox.client.close()

    @respx.mock
    def test_iter_file_not_found(self, mock_env_clear, mock_sandbox_get_response):
        """Test streamed file read raises for non-existent file."""
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

        respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/fs/read").mock(
            return_value=httpx.Response(404, json={"error": {"code": "not_found"}})
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        with pytest.raises(SandboxNotFoundError, match="HTTP 404"):
            sandbox.iter_file("/nonexistent/file")

        sandbox.client.close()

    @respx.mock
    def test_download_file_sync(
        self, mock_env_clear, mock_sandbox_get_response, mock_sandbox_read_file_content, tmp_path
    ):
        """Test synchronous sandbox-to-local file download."""
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

        respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/fs/read").mock(
            return_value=httpx.Response(200, content=mock_sandbox_read_file_content)
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        destination = tmp_path / "downloaded.txt"
        result = sandbox.download_file("/etc/hosts", destination)

        assert result == str(destination.resolve())
        assert destination.read_bytes() == mock_sandbox_read_file_content

        sandbox.client.close()

    @respx.mock
    def test_download_file_sync_creates_parents(
        self, mock_env_clear, mock_sandbox_get_response, mock_sandbox_read_file_content, tmp_path
    ):
        """Test sync download can create parent directories."""
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

        respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/fs/read").mock(
            return_value=httpx.Response(200, content=mock_sandbox_read_file_content)
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        destination = tmp_path / "nested" / "dir" / "downloaded.txt"
        result = sandbox.download_file("/etc/hosts", destination, create_parents=True)

        assert result == str(destination.resolve())
        assert destination.read_bytes() == mock_sandbox_read_file_content

        sandbox.client.close()

    @respx.mock
    def test_download_file_sync_uses_filesystem_client_for_local_path_setup(
        self, mock_env_clear, mock_sandbox_get_response, mock_sandbox_read_file_content, tmp_path
    ):
        """Test sync download uses the injected filesystem client for local setup."""
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

        respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/fs/read").mock(
            return_value=httpx.Response(200, content=mock_sandbox_read_file_content)
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        requested_destination = tmp_path / "ignored.txt"
        rewritten_destination = tmp_path / "rewritten" / "downloaded.txt"
        sandbox.client._filesystem_client.coerce_path = AsyncMock(
            return_value=str(rewritten_destination)
        )

        async def create_parent_directories(path: str) -> None:
            Path(path).parent.mkdir(parents=True, exist_ok=True)

        sandbox.client._filesystem_client.create_parent_directories = AsyncMock(
            side_effect=create_parent_directories
        )

        result = sandbox.download_file("/etc/hosts", requested_destination, create_parents=True)

        assert result == str(rewritten_destination.resolve())
        assert rewritten_destination.read_bytes() == mock_sandbox_read_file_content
        assert not requested_destination.exists()
        sandbox.client._filesystem_client.coerce_path.assert_awaited_once_with(
            requested_destination
        )
        sandbox.client._filesystem_client.create_parent_directories.assert_awaited_once_with(
            str(rewritten_destination.resolve())
        )

        sandbox.client.close()

    @respx.mock
    def test_download_file_sync_stream_failure_removes_part_file(
        self, mock_env_clear, mock_sandbox_get_response, tmp_path
    ):
        """Test sync download cleans up the temp file after a stream failure."""
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

        respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/fs/read").mock(
            return_value=httpx.Response(200, content=b"ignored")
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        async def fail_after_partial_chunk(
            response: httpx.Response, *, chunk_size: int
        ) -> AsyncIterator[bytes]:
            del response, chunk_size
            yield b"partial"
            raise RuntimeError("stream failed")

        sandbox.client._stream_file_chunks = fail_after_partial_chunk  # type: ignore[method-assign]

        destination = tmp_path / "downloaded.txt"
        temp_path = destination.with_name(destination.name + ".part")

        with pytest.raises(RuntimeError, match="stream failed"):
            sandbox.download_file("/etc/hosts", destination)

        assert not destination.exists()
        assert not temp_path.exists()

        sandbox.client.close()

    @respx.mock
    def test_download_file_sync_not_found(
        self, mock_env_clear, mock_sandbox_get_response, tmp_path
    ):
        """Test sync download raises for non-existent file."""
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

        respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/fs/read").mock(
            return_value=httpx.Response(404, json={"error": {"code": "not_found"}})
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        destination = tmp_path / "downloaded.txt"
        with pytest.raises(SandboxNotFoundError, match="HTTP 404"):
            sandbox.download_file("/nonexistent/file", destination)
        assert not destination.exists()

        sandbox.client.close()

    @respx.mock
    def test_download_file_sync_server_error_propagates(
        self, mock_env_clear, mock_sandbox_get_response, tmp_path
    ):
        """Test sync download preserves non-404 sandbox API errors."""
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

        respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/fs/read").mock(
            return_value=httpx.Response(500, json={"message": "remote exploded"})
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        destination = tmp_path / "downloaded.txt"

        with pytest.raises(SandboxServerError, match="HTTP 500: remote exploded"):
            sandbox.download_file("/etc/hosts", destination)

        assert not destination.exists()

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
        assert content == mock_sandbox_read_file_content

        await sandbox.client.aclose()

    @respx.mock
    @pytest.mark.asyncio
    async def test_iter_file_async(
        self, mock_env_clear, mock_sandbox_get_response, mock_sandbox_read_file_content
    ):
        """Test asynchronous streamed file read."""
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

        respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/fs/read").mock(
            return_value=httpx.Response(200, content=mock_sandbox_read_file_content)
        )

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        stream = await sandbox.iter_file("/etc/hosts", chunk_size=4)

        assert stream is not None
        chunks = [chunk async for chunk in stream]
        assert b"".join(chunks) == mock_sandbox_read_file_content

        await sandbox.client.aclose()

    @respx.mock
    @pytest.mark.asyncio
    async def test_read_file_async_not_found(self, mock_env_clear, mock_sandbox_get_response):
        """Test async file read raises for non-existent file."""
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

        respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/fs/read").mock(
            return_value=httpx.Response(404, json={"error": {"code": "not_found"}})
        )

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        with pytest.raises(SandboxNotFoundError, match="HTTP 404"):
            await sandbox.read_file("/nonexistent/file")

        await sandbox.client.aclose()

    @respx.mock
    @pytest.mark.asyncio
    async def test_iter_file_async_not_found(self, mock_env_clear, mock_sandbox_get_response):
        """Test async streamed file read raises for non-existent file."""
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

        respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/fs/read").mock(
            return_value=httpx.Response(404, json={"error": {"code": "not_found"}})
        )

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        with pytest.raises(SandboxNotFoundError, match="HTTP 404"):
            await sandbox.iter_file("/nonexistent/file")

        await sandbox.client.aclose()

    @respx.mock
    @pytest.mark.asyncio
    async def test_download_file_async(
        self, mock_env_clear, mock_sandbox_get_response, mock_sandbox_read_file_content, tmp_path
    ):
        """Test async sandbox-to-local file download."""
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

        respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/fs/read").mock(
            return_value=httpx.Response(200, content=mock_sandbox_read_file_content)
        )

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        destination = tmp_path / "downloaded.txt"
        result = await sandbox.download_file("/etc/hosts", destination)

        assert result == str(destination.resolve())
        assert destination.read_bytes() == mock_sandbox_read_file_content

        await sandbox.client.aclose()

    @respx.mock
    @pytest.mark.asyncio
    async def test_download_file_async_creates_parents(
        self, mock_env_clear, mock_sandbox_get_response, mock_sandbox_read_file_content, tmp_path
    ):
        """Test async download can create parent directories."""
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

        respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/fs/read").mock(
            return_value=httpx.Response(200, content=mock_sandbox_read_file_content)
        )

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        destination = tmp_path / "nested" / "dir" / "downloaded.txt"
        result = await sandbox.download_file("/etc/hosts", destination, create_parents=True)

        assert result == str(destination.resolve())
        assert destination.parent.is_dir()
        assert destination.read_bytes() == mock_sandbox_read_file_content

        await sandbox.client.aclose()

    @respx.mock
    @pytest.mark.asyncio
    async def test_download_file_async_uses_filesystem_client_for_local_path_setup(
        self, mock_env_clear, mock_sandbox_get_response, mock_sandbox_read_file_content, tmp_path
    ):
        """Test async download uses the injected filesystem client for local setup."""
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

        respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/fs/read").mock(
            return_value=httpx.Response(200, content=mock_sandbox_read_file_content)
        )

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        requested_destination = tmp_path / "ignored.txt"
        rewritten_destination = tmp_path / "rewritten" / "downloaded.txt"
        sandbox.client._filesystem_client.coerce_path = AsyncMock(
            return_value=str(rewritten_destination)
        )

        async def create_parent_directories(path: str) -> None:
            Path(path).parent.mkdir(parents=True, exist_ok=True)

        sandbox.client._filesystem_client.create_parent_directories = AsyncMock(
            side_effect=create_parent_directories
        )

        result = await sandbox.download_file(
            "/etc/hosts", requested_destination, create_parents=True
        )

        assert result == str(rewritten_destination.resolve())
        assert rewritten_destination.read_bytes() == mock_sandbox_read_file_content
        assert not requested_destination.exists()
        sandbox.client._filesystem_client.coerce_path.assert_awaited_once_with(
            requested_destination
        )
        sandbox.client._filesystem_client.create_parent_directories.assert_awaited_once_with(
            str(rewritten_destination.resolve())
        )

        await sandbox.client.aclose()

    @respx.mock
    @pytest.mark.asyncio
    async def test_download_file_async_uses_filesystem_client_for_file_writes_and_cleanup(
        self, mock_env_clear, mock_sandbox_get_response, tmp_path
    ):
        """Test async download routes writes through the filesystem client and cleans up."""
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

        respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/fs/read").mock(
            return_value=httpx.Response(200, content=b"ignored")
        )

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        async def stream_chunks(
            response: httpx.Response, *, chunk_size: int
        ) -> AsyncIterator[bytes]:
            del response, chunk_size
            yield b"first"
            yield b"second"

        sandbox.client._stream_file_chunks = stream_chunks  # type: ignore[method-assign]

        destination = tmp_path / "downloaded.txt"
        resolved_destination = destination.resolve()
        temp_path = Path(str(resolved_destination) + ".part")

        original_open = sandbox.client._filesystem_client.open_binary_writer
        original_close = sandbox.client._filesystem_client.close
        original_remove_if_exists = sandbox.client._filesystem_client.remove_if_exists

        sandbox.client._filesystem_client.open_binary_writer = AsyncMock(side_effect=original_open)
        sandbox.client._filesystem_client.close = AsyncMock(side_effect=original_close)
        sandbox.client._filesystem_client.remove_if_exists = AsyncMock(
            side_effect=original_remove_if_exists
        )

        original_write = sandbox.client._filesystem_client.write

        async def fail_during_write(handle: object, data: bytes) -> None:
            await original_write(handle, data)
            raise RuntimeError("write failed")

        sandbox.client._filesystem_client.write = AsyncMock(side_effect=fail_during_write)

        with pytest.raises(RuntimeError, match="write failed"):
            await sandbox.download_file("/etc/hosts", destination)

        assert not destination.exists()
        assert not temp_path.exists()
        sandbox.client._filesystem_client.open_binary_writer.assert_awaited_once_with(
            str(temp_path)
        )
        sandbox.client._filesystem_client.write.assert_awaited_once()
        sandbox.client._filesystem_client.close.assert_awaited_once()
        sandbox.client._filesystem_client.remove_if_exists.assert_awaited_once_with(str(temp_path))

        await sandbox.client.aclose()

    @respx.mock
    @pytest.mark.asyncio
    async def test_download_file_async_not_found(
        self, mock_env_clear, mock_sandbox_get_response, tmp_path
    ):
        """Test async download raises for non-existent file."""
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

        respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/fs/read").mock(
            return_value=httpx.Response(404, json={"error": {"code": "not_found"}})
        )

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        destination = tmp_path / "downloaded.txt"
        with pytest.raises(SandboxNotFoundError, match="HTTP 404"):
            await sandbox.download_file("/nonexistent/file", destination)
        assert not destination.exists()

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
        """Test synchronous sandbox stop exposes the stop lifecycle without blocking."""
        from vercel.sandbox import Sandbox

        sandbox_id = "sbx_test123456"
        stopping_response = {**mock_sandbox_get_response, "status": "stopping"}
        stopped_response = {
            **mock_sandbox_get_response,
            "status": "stopped",
            "stoppedAt": mock_sandbox_get_response["updatedAt"] + 1,
        }

        get_route = respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
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
                        "sandbox": stopped_response,
                        "routes": [],
                    },
                ),
            ]
        )

        # Mock stop
        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/stop").mock(
            return_value=httpx.Response(200, json={"sandbox": stopping_response})
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        sandbox.stop()

        assert route.called
        assert sandbox.status == "stopping"

        sandbox.refresh()

        assert sandbox.status == "stopped"
        assert get_route.call_count == 2

        sandbox.client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_stop_async(self, mock_env_clear, mock_sandbox_get_response):
        """Test asynchronous sandbox stop exposes the stop lifecycle without blocking."""
        from vercel.sandbox import AsyncSandbox

        sandbox_id = "sbx_test123456"
        stopping_response = {**mock_sandbox_get_response, "status": "stopping"}
        stopped_response = {
            **mock_sandbox_get_response,
            "status": "stopped",
            "stoppedAt": mock_sandbox_get_response["updatedAt"] + 1,
        }

        get_route = respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
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
                        "sandbox": stopped_response,
                        "routes": [],
                    },
                ),
            ]
        )

        # Mock stop
        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/stop").mock(
            return_value=httpx.Response(200, json={"sandbox": stopping_response})
        )

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        await sandbox.stop()

        assert route.called
        assert sandbox.status == "stopping"

        await sandbox.refresh()

        assert sandbox.status == "stopped"
        assert get_route.call_count == 2

        await sandbox.client.aclose()

    @respx.mock
    def test_stop_sync_blocking_polls_until_stopped(
        self, mock_env_clear, mock_sandbox_get_response
    ):
        """Test blocking sync stop polls until the sandbox is stopped."""
        from vercel.sandbox import Sandbox

        sandbox_id = "sbx_test123456"
        stopping_response = {**mock_sandbox_get_response, "status": "stopping"}
        stopped_response = {
            **mock_sandbox_get_response,
            "status": "stopped",
            "stoppedAt": mock_sandbox_get_response["updatedAt"] + 1,
        }

        get_route = respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            side_effect=[
                httpx.Response(200, json={"sandbox": mock_sandbox_get_response, "routes": []}),
                httpx.Response(200, json={"sandbox": stopping_response, "routes": []}),
                httpx.Response(200, json={"sandbox": stopped_response, "routes": []}),
            ]
        )
        stop_route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/stop").mock(
            return_value=httpx.Response(200, json={"sandbox": stopping_response})
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        sandbox.stop(blocking=True, poll_interval=0.01)

        assert stop_route.called
        assert sandbox.status == "stopped"
        assert get_route.call_count == 3

        sandbox.client.close()

    @respx.mock
    def test_stop_sync_blocking_timeout(self, mock_env_clear, mock_sandbox_get_response):
        """Test blocking sync stop raises TimeoutError when stop never completes."""
        from vercel.sandbox import Sandbox

        sandbox_id = "sbx_test123456"
        stopping_response = {**mock_sandbox_get_response, "status": "stopping"}

        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            side_effect=[
                httpx.Response(200, json={"sandbox": mock_sandbox_get_response, "routes": []}),
                httpx.Response(200, json={"sandbox": stopping_response, "routes": []}),
                httpx.Response(200, json={"sandbox": stopping_response, "routes": []}),
                httpx.Response(200, json={"sandbox": stopping_response, "routes": []}),
                httpx.Response(200, json={"sandbox": stopping_response, "routes": []}),
                httpx.Response(200, json={"sandbox": stopping_response, "routes": []}),
            ]
        )
        stop_route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/stop").mock(
            return_value=httpx.Response(200, json={"sandbox": stopping_response})
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        with pytest.raises(TimeoutError, match="did not reach 'stopped' status"):
            sandbox.stop(blocking=True, timeout=0.05, poll_interval=0.01)

        assert stop_route.called
        assert sandbox.status == "stopping"

        sandbox.client.close()

    @respx.mock
    def test_stop_sync_blocking_stops_after_first_stopped_refresh(
        self, mock_env_clear, mock_sandbox_get_response
    ):
        """Test blocking sync stop exits after the first stopped refresh."""
        from vercel.sandbox import Sandbox

        sandbox_id = "sbx_test123456"
        stopping_response = {**mock_sandbox_get_response, "status": "stopping"}
        stopped_response = {
            **mock_sandbox_get_response,
            "status": "stopped",
            "stoppedAt": mock_sandbox_get_response["updatedAt"] + 1,
        }

        get_route = respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            side_effect=[
                httpx.Response(200, json={"sandbox": mock_sandbox_get_response, "routes": []}),
                httpx.Response(200, json={"sandbox": stopped_response, "routes": []}),
            ]
        )
        stop_route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/stop").mock(
            return_value=httpx.Response(200, json={"sandbox": stopping_response})
        )

        sandbox = Sandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        sandbox.stop(blocking=True, poll_interval=0.01)

        assert stop_route.called
        assert sandbox.status == "stopped"
        assert get_route.call_count == 2

        sandbox.client.close()

    @respx.mock
    @pytest.mark.asyncio
    async def test_stop_async_blocking_polls_until_stopped(
        self, mock_env_clear, mock_sandbox_get_response
    ):
        """Test blocking async stop polls until the sandbox is stopped."""
        from vercel.sandbox import AsyncSandbox

        sandbox_id = "sbx_test123456"
        stopping_response = {**mock_sandbox_get_response, "status": "stopping"}
        stopped_response = {
            **mock_sandbox_get_response,
            "status": "stopped",
            "stoppedAt": mock_sandbox_get_response["updatedAt"] + 1,
        }

        get_route = respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            side_effect=[
                httpx.Response(200, json={"sandbox": mock_sandbox_get_response, "routes": []}),
                httpx.Response(200, json={"sandbox": stopping_response, "routes": []}),
                httpx.Response(200, json={"sandbox": stopped_response, "routes": []}),
            ]
        )
        stop_route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/stop").mock(
            return_value=httpx.Response(200, json={"sandbox": stopping_response})
        )

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        await sandbox.stop(blocking=True, poll_interval=0.01)

        assert stop_route.called
        assert sandbox.status == "stopped"
        assert get_route.call_count == 3

        await sandbox.client.aclose()

    @respx.mock
    @pytest.mark.asyncio
    async def test_stop_async_blocking_timeout(self, mock_env_clear, mock_sandbox_get_response):
        """Test blocking async stop raises TimeoutError when stop never completes."""
        from vercel.sandbox import AsyncSandbox

        sandbox_id = "sbx_test123456"
        stopping_response = {**mock_sandbox_get_response, "status": "stopping"}

        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            side_effect=[
                httpx.Response(200, json={"sandbox": mock_sandbox_get_response, "routes": []}),
                httpx.Response(200, json={"sandbox": stopping_response, "routes": []}),
                httpx.Response(200, json={"sandbox": stopping_response, "routes": []}),
                httpx.Response(200, json={"sandbox": stopping_response, "routes": []}),
                httpx.Response(200, json={"sandbox": stopping_response, "routes": []}),
                httpx.Response(200, json={"sandbox": stopping_response, "routes": []}),
            ]
        )
        stop_route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/stop").mock(
            return_value=httpx.Response(200, json={"sandbox": stopping_response})
        )

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        with pytest.raises(TimeoutError, match="did not reach 'stopped' status"):
            await sandbox.stop(blocking=True, timeout=0.05, poll_interval=0.01)

        assert stop_route.called
        assert sandbox.status == "stopping"

        await sandbox.client.aclose()

    @respx.mock
    @pytest.mark.asyncio
    async def test_stop_async_blocking_stops_after_first_stopped_refresh(
        self, mock_env_clear, mock_sandbox_get_response
    ):
        """Test blocking async stop exits after the first stopped refresh."""
        from vercel.sandbox import AsyncSandbox

        sandbox_id = "sbx_test123456"
        stopping_response = {**mock_sandbox_get_response, "status": "stopping"}
        stopped_response = {
            **mock_sandbox_get_response,
            "status": "stopped",
            "stoppedAt": mock_sandbox_get_response["updatedAt"] + 1,
        }

        get_route = respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            side_effect=[
                httpx.Response(200, json={"sandbox": mock_sandbox_get_response, "routes": []}),
                httpx.Response(200, json={"sandbox": stopped_response, "routes": []}),
            ]
        )
        stop_route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/stop").mock(
            return_value=httpx.Response(200, json={"sandbox": stopping_response})
        )

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        await sandbox.stop(blocking=True, poll_interval=0.01)

        assert stop_route.called
        assert sandbox.status == "stopped"
        assert get_route.call_count == 2

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
    def test_create_snapshot_sync_without_expiration(
        self, mock_env_clear, mock_sandbox_get_response, mock_sandbox_snapshot_response
    ):
        """Test sync snapshot creation omits the body when expiration is unset."""
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
        assert route.calls.last.request.content == b""
        assert snapshot.snapshot_id == mock_sandbox_snapshot_response["id"]
        assert snapshot.created_at == mock_sandbox_snapshot_response["createdAt"]
        assert snapshot.expires_at == mock_sandbox_snapshot_response["expiresAt"]
        # Sandbox should be stopped after snapshot
        assert sandbox.status == "stopped"

        sandbox.client.close()

    @respx.mock
    def test_create_snapshot_sync_with_expiration(
        self, mock_env_clear, mock_sandbox_get_response, mock_sandbox_snapshot_response
    ):
        """Test sync snapshot creation forwards a non-zero expiration."""
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

        snapshot = sandbox.snapshot(expiration=86_400_000)

        assert route.called
        body = json.loads(route.calls.last.request.content)
        assert body == {"expiration": 86_400_000}
        assert snapshot.created_at == mock_sandbox_snapshot_response["createdAt"]
        assert sandbox.status == "stopped"

        sandbox.client.close()

    @respx.mock
    def test_create_snapshot_sync_with_zero_expiration(
        self, mock_env_clear, mock_sandbox_get_response, mock_sandbox_snapshot_response
    ):
        """Test sync snapshot creation preserves explicit zero expiration."""
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

        sandbox.snapshot(expiration=0)

        assert route.called
        body = json.loads(route.calls.last.request.content)
        assert body == {"expiration": 0}
        assert sandbox.status == "stopped"

        sandbox.client.close()

    @respx.mock
    async def test_create_snapshot_async_without_expiration(
        self, mock_env_clear, mock_sandbox_get_response, mock_sandbox_snapshot_response
    ):
        """Test async snapshot creation omits the body when expiration is unset."""
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

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        snapshot = await sandbox.snapshot()

        assert route.called
        assert route.calls.last.request.content == b""
        assert snapshot.snapshot_id == mock_sandbox_snapshot_response["id"]
        assert snapshot.created_at == mock_sandbox_snapshot_response["createdAt"]
        assert snapshot.expires_at == mock_sandbox_snapshot_response["expiresAt"]
        assert sandbox.status == "stopped"

        await sandbox.client.aclose()

    @respx.mock
    @pytest.mark.asyncio
    async def test_create_snapshot_async_with_expiration(
        self, mock_env_clear, mock_sandbox_get_response, mock_sandbox_snapshot_response
    ):
        """Test async snapshot creation forwards a non-zero expiration."""
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

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        snapshot = await sandbox.snapshot(expiration=86_400_000)

        assert route.called
        body = json.loads(route.calls.last.request.content)
        assert body == {"expiration": 86_400_000}
        assert snapshot.created_at == mock_sandbox_snapshot_response["createdAt"]
        assert sandbox.status == "stopped"

        await sandbox.client.aclose()

    @respx.mock
    @pytest.mark.asyncio
    async def test_create_snapshot_async_with_zero_expiration_and_optional_expires_at(
        self, mock_env_clear, mock_sandbox_get_response, mock_sandbox_snapshot_response
    ):
        """Test async snapshot creation keeps zero expiration and optional expiry metadata."""
        from vercel.sandbox import AsyncSandbox

        sandbox_id = "sbx_test123456"
        snapshot_response = dict(mock_sandbox_snapshot_response)
        snapshot_response.pop("expiresAt")

        respx.get(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": mock_sandbox_get_response,
                    "routes": [],
                },
            )
        )

        stopped_response = dict(mock_sandbox_get_response)
        stopped_response["status"] = "stopped"
        route = respx.post(f"{SANDBOX_API_BASE}/v1/sandboxes/{sandbox_id}/snapshot").mock(
            return_value=httpx.Response(
                200,
                json={
                    "sandbox": stopped_response,
                    "snapshot": snapshot_response,
                },
            )
        )

        sandbox = await AsyncSandbox.get(
            sandbox_id=sandbox_id,
            token="test_token",
            team_id="team_test123",
            project_id="prj_test123",
        )

        snapshot = await sandbox.snapshot(expiration=0)

        assert route.called
        body = json.loads(route.calls.last.request.content)
        assert body == {"expiration": 0}
        assert snapshot.created_at == snapshot_response["createdAt"]
        assert snapshot.expires_at is None
        assert sandbox.status == "stopped"

        await sandbox.client.aclose()


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
