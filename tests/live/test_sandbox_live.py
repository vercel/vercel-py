"""Live API tests for Vercel Sandbox.

These tests make real API calls and require VERCEL_TOKEN and VERCEL_TEAM_ID environment variables.
Run with: pytest tests/live/test_sandbox_live.py -v
"""

import pytest

from .conftest import requires_sandbox_credentials


@requires_sandbox_credentials
@pytest.mark.live
class TestSandboxLive:
    """Live tests for Sandbox API operations."""

    def test_create_run_stop_lifecycle(self, vercel_token, vercel_team_id, cleanup_registry):
        """Test complete sandbox create -> run command -> stop lifecycle."""
        from vercel.sandbox import Sandbox

        # Create sandbox
        sandbox = Sandbox.create(
            token=vercel_token,
            team_id=vercel_team_id,
        )
        cleanup_registry.register("sandbox", sandbox.sandbox_id)

        try:
            # Verify creation
            assert sandbox.sandbox_id is not None
            assert sandbox.status == "running"

            # Run a simple command
            result = sandbox.run_command("echo", ["Hello from sandbox"])

            assert result.exit_code == 0
            assert "Hello from sandbox" in result.stdout

            # Stop the sandbox
            sandbox.stop()
        finally:
            # Ensure cleanup
            try:
                sandbox.stop()
            except Exception:
                pass
            sandbox.client.close()

    @pytest.mark.asyncio
    async def test_async_sandbox_lifecycle(self, vercel_token, vercel_team_id, cleanup_registry):
        """Test async sandbox create -> run command -> stop lifecycle."""
        from vercel.sandbox import AsyncSandbox

        # Create sandbox using async context manager
        async with await AsyncSandbox.create(
            token=vercel_token,
            team_id=vercel_team_id,
        ) as sandbox:
            cleanup_registry.register("sandbox", sandbox.sandbox_id)

            # Verify creation
            assert sandbox.sandbox_id is not None
            assert sandbox.status == "running"

            # Run a simple command
            result = await sandbox.run_command("echo", ["Async hello"])

            assert result.exit_code == 0
            assert "Async hello" in result.stdout

        # Context manager should have stopped the sandbox

    def test_file_operations(self, vercel_token, vercel_team_id, cleanup_registry):
        """Test sandbox file write and read operations."""
        from vercel.sandbox import Sandbox
        from vercel.sandbox.models import WriteFile

        sandbox = Sandbox.create(
            token=vercel_token,
            team_id=vercel_team_id,
        )
        cleanup_registry.register("sandbox", sandbox.sandbox_id)

        try:
            # Write a file
            test_content = "Hello, this is test content!"
            sandbox.write_files([WriteFile(path="/tmp/test.txt", content=test_content.encode())])

            # Read the file back
            content = sandbox.read_file("/tmp/test.txt")

            assert content is not None
            assert test_content in content.decode()

            # Read a non-existent file
            missing = sandbox.read_file("/tmp/nonexistent.txt")
            assert missing is None

        finally:
            try:
                sandbox.stop()
            except Exception:
                pass
            sandbox.client.close()

    def test_run_command_with_env(self, vercel_token, vercel_team_id, cleanup_registry):
        """Test running command with environment variables."""
        from vercel.sandbox import Sandbox

        sandbox = Sandbox.create(
            token=vercel_token,
            team_id=vercel_team_id,
        )
        cleanup_registry.register("sandbox", sandbox.sandbox_id)

        try:
            # Run command with custom env
            result = sandbox.run_command(
                "sh",
                ["-c", "echo $MY_VAR"],
                env={"MY_VAR": "test_value_123"},
            )

            assert result.exit_code == 0
            assert "test_value_123" in result.stdout

        finally:
            try:
                sandbox.stop()
            except Exception:
                pass
            sandbox.client.close()

    def test_run_command_detached(self, vercel_token, vercel_team_id, cleanup_registry):
        """Test running a detached command."""
        from vercel.sandbox import Sandbox

        sandbox = Sandbox.create(
            token=vercel_token,
            team_id=vercel_team_id,
        )
        cleanup_registry.register("sandbox", sandbox.sandbox_id)

        try:
            # Run a detached command (doesn't wait for completion)
            command = sandbox.run_command_detached("sleep", ["1"])

            assert command.cmd_id is not None

            # Wait for it to complete
            finished = command.wait()
            assert finished.exit_code == 0

        finally:
            try:
                sandbox.stop()
            except Exception:
                pass
            sandbox.client.close()

    def test_context_manager(self, vercel_token, vercel_team_id, cleanup_registry):
        """Test sandbox context manager cleanup."""
        from vercel.sandbox import Sandbox

        with Sandbox.create(
            token=vercel_token,
            team_id=vercel_team_id,
        ) as sandbox:
            cleanup_registry.register("sandbox", sandbox.sandbox_id)

            # Run a command inside context
            result = sandbox.run_command("whoami")
            assert result.exit_code == 0

        # Context manager should have stopped the sandbox

    def test_get_existing_sandbox(self, vercel_token, vercel_team_id, cleanup_registry):
        """Test getting an existing sandbox by ID."""
        from vercel.sandbox import Sandbox

        # Create a sandbox
        original = Sandbox.create(
            token=vercel_token,
            team_id=vercel_team_id,
        )
        cleanup_registry.register("sandbox", original.sandbox_id)

        try:
            # Get the same sandbox by ID
            fetched = Sandbox.get(
                sandbox_id=original.sandbox_id,
                token=vercel_token,
                team_id=vercel_team_id,
            )

            assert fetched.sandbox_id == original.sandbox_id
            assert fetched.status == "running"

            fetched.client.close()
        finally:
            try:
                original.stop()
            except Exception:
                pass
            original.client.close()

    def test_mk_dir(self, vercel_token, vercel_team_id, cleanup_registry):
        """Test creating a directory in the sandbox."""
        from vercel.sandbox import Sandbox

        sandbox = Sandbox.create(
            token=vercel_token,
            team_id=vercel_team_id,
        )
        cleanup_registry.register("sandbox", sandbox.sandbox_id)

        try:
            # Create a directory
            sandbox.mk_dir("/tmp/test-dir")

            # Verify it exists by running ls
            result = sandbox.run_command("ls", ["-la", "/tmp/test-dir"])
            assert result.exit_code == 0

        finally:
            try:
                sandbox.stop()
            except Exception:
                pass
            sandbox.client.close()
