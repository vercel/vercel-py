"""Fixtures for live API tests.

These tests require real API credentials set via environment variables:
- VERCEL_TOKEN: Vercel API token
- VERCEL_TEAM_ID: Vercel team ID
- BLOB_READ_WRITE_TOKEN: Blob storage read/write token
"""

import os
import time
import uuid
from collections.abc import Generator
from typing import Any

import pytest


def has_vercel_credentials() -> bool:
    """Check if Vercel API credentials are available."""
    return bool(os.getenv("VERCEL_TOKEN") and os.getenv("VERCEL_TEAM_ID"))


def has_blob_credentials() -> bool:
    """Check if Blob storage credentials are available."""
    return bool(os.getenv("BLOB_READ_WRITE_TOKEN"))


def has_sandbox_credentials() -> bool:
    """Check if Sandbox credentials are available."""
    return has_vercel_credentials()


# Skip markers for live tests
requires_vercel_credentials = pytest.mark.skipif(
    not has_vercel_credentials(),
    reason="Requires VERCEL_TOKEN and VERCEL_TEAM_ID environment variables",
)

requires_blob_credentials = pytest.mark.skipif(
    not has_blob_credentials(),
    reason="Requires BLOB_READ_WRITE_TOKEN environment variable",
)

requires_sandbox_credentials = pytest.mark.skipif(
    not has_sandbox_credentials(),
    reason="Requires VERCEL_TOKEN and VERCEL_TEAM_ID environment variables for sandbox",
)


@pytest.fixture
def vercel_token() -> str:
    """Get Vercel API token from environment."""
    token = os.getenv("VERCEL_TOKEN")
    if not token:
        pytest.skip("VERCEL_TOKEN environment variable not set")
    return token


@pytest.fixture
def vercel_team_id() -> str:
    """Get Vercel team ID from environment."""
    team_id = os.getenv("VERCEL_TEAM_ID")
    if not team_id:
        pytest.skip("VERCEL_TEAM_ID environment variable not set")
    return team_id


@pytest.fixture
def blob_token() -> str:
    """Get Blob storage token from environment."""
    token = os.getenv("BLOB_READ_WRITE_TOKEN")
    if not token:
        pytest.skip("BLOB_READ_WRITE_TOKEN environment variable not set")
    return token


@pytest.fixture
def unique_test_name() -> str:
    """Generate a unique test resource name with timestamp.

    Format: vercel-py-test-{timestamp}-{uuid}
    """
    timestamp = int(time.time())
    unique_id = uuid.uuid4().hex[:8]
    return f"vercel-py-test-{timestamp}-{unique_id}"


@pytest.fixture
def unique_blob_path() -> str:
    """Generate a unique blob path for testing.

    Format: test/{timestamp}-{uuid}/file.txt
    """
    timestamp = int(time.time())
    unique_id = uuid.uuid4().hex[:8]
    return f"test/{timestamp}-{unique_id}/file.txt"


class CleanupRegistry:
    """Registry for tracking resources that need cleanup after tests."""

    def __init__(self) -> None:
        self._cleanups: list[tuple[str, Any]] = []

    def register(self, resource_type: str, resource_id: Any) -> None:
        """Register a resource for cleanup.

        Args:
            resource_type: Type of resource (e.g., "blob", "project", "sandbox")
            resource_id: Identifier for the resource
        """
        self._cleanups.append((resource_type, resource_id))

    def get_resources(self, resource_type: str) -> list[Any]:
        """Get all registered resources of a specific type."""
        return [rid for rtype, rid in self._cleanups if rtype == resource_type]

    def clear(self) -> None:
        """Clear all registered resources."""
        self._cleanups.clear()


@pytest.fixture
def cleanup_registry() -> Generator[CleanupRegistry, None, None]:
    """Fixture providing a cleanup registry for tracking test resources.

    Usage:
        def test_create_resource(cleanup_registry, blob_token):
            result = put("test.txt", b"data", token=blob_token)
            cleanup_registry.register("blob", result.url)
            # Test continues...
            # Cleanup happens automatically after test
    """
    registry = CleanupRegistry()
    yield registry

    # Cleanup blob resources
    blob_urls = registry.get_resources("blob")
    if blob_urls:
        try:
            from vercel.blob import delete

            blob_token = os.getenv("BLOB_READ_WRITE_TOKEN")
            if blob_token:
                for url in blob_urls:
                    try:
                        delete(url, token=blob_token)
                    except Exception:
                        pass  # Best effort cleanup
        except ImportError:
            pass

    # Cleanup project resources
    project_ids = registry.get_resources("project")
    if project_ids:
        try:
            from vercel.projects import delete_project

            vercel_token = os.getenv("VERCEL_TOKEN")
            team_id = os.getenv("VERCEL_TEAM_ID")
            if vercel_token and team_id:
                for project_id in project_ids:
                    try:
                        delete_project(project_id, token=vercel_token, team_id=team_id)
                    except Exception:
                        pass  # Best effort cleanup
        except ImportError:
            pass

    # Cleanup sandbox resources
    sandbox_ids = registry.get_resources("sandbox")
    if sandbox_ids:
        try:
            from vercel.sandbox import Sandbox

            vercel_token = os.getenv("VERCEL_TOKEN")
            team_id = os.getenv("VERCEL_TEAM_ID")
            if vercel_token and team_id:
                for sandbox_id in sandbox_ids:
                    try:
                        sandbox = Sandbox.get(
                            sandbox_id=sandbox_id,
                            token=vercel_token,
                            team_id=team_id,
                        )
                        sandbox.stop()
                    except Exception:
                        pass  # Best effort cleanup
        except ImportError:
            pass

    registry.clear()
