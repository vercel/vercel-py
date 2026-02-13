"""Shared fixtures for all tests."""

import os
import time
import uuid
from collections.abc import Generator

import pytest


@pytest.fixture
def mock_env_clear(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    """Clear all Vercel-related environment variables for testing.

    This ensures tests don't accidentally use real credentials from the environment.
    """
    env_vars_to_clear = [
        # General Vercel
        "VERCEL_TOKEN",
        "VERCEL_TEAM_ID",
        "VERCEL_PROJECT_ID",
        # Blob storage
        "BLOB_READ_WRITE_TOKEN",
        "BLOB_STORE_ID",
        # OIDC
        "VERCEL_OIDC_TOKEN",
        "VERCEL_OIDC_TOKEN_HEADER",
        # Cache
        "VERCEL_CACHE_API_TOKEN",
        "VERCEL_CACHE_API_URL",
        # Functions
        "VERCEL_URL",
        "VERCEL_ENV",
        "VERCEL_REGION",
    ]
    for var in env_vars_to_clear:
        monkeypatch.delenv(var, raising=False)
    yield


@pytest.fixture
def mock_token() -> str:
    """Mock Vercel API token for testing."""
    return "test_token_123456789"


@pytest.fixture
def mock_team_id() -> str:
    """Mock Vercel team ID for testing."""
    return "team_test123456789"


@pytest.fixture
def mock_project_id() -> str:
    """Mock Vercel project ID for testing."""
    return "prj_test123456789"


@pytest.fixture
def mock_blob_token() -> str:
    """Mock blob storage token for testing."""
    return "vercel_blob_rw_test_token_123456789"


@pytest.fixture
def unique_test_name() -> str:
    """Generate a unique test resource name with timestamp."""
    timestamp = int(time.time())
    unique_id = uuid.uuid4().hex[:8]
    return f"vercel-py-test-{timestamp}-{unique_id}"


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
