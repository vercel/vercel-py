"""Integration tests for Vercel Sandbox API using respx mocking.

Tests both sync and async variants (Sandbox and AsyncSandbox).
"""

import httpx
import pytest
import respx

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
