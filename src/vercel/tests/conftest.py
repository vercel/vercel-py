"""Shared fixtures for all tests."""

import importlib.util
import os
import time
import uuid
from collections.abc import Generator
from pathlib import Path

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

    from vercel.headers import set_headers
    from vercel.oidc.token import _clear_cached_oidc_token

    set_headers(None)
    _clear_cached_oidc_token()
    yield
    _clear_cached_oidc_token()
    set_headers(None)


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


# Skip markers for live tests
requires_vercel_credentials = pytest.mark.skipif(
    not has_vercel_credentials(),
    reason="Requires VERCEL_TOKEN and VERCEL_TEAM_ID environment variables",
)

requires_blob_credentials = pytest.mark.skipif(
    not has_blob_credentials(),
    reason="Requires BLOB_READ_WRITE_TOKEN environment variable",
)

# Workflow tests import the workflow worlds, which depend on vercel-workers
# (installed only on Python >= 3.12; see pyproject). Skip collecting them when the
# package is unavailable, so collection doesn't error on older interpreters.
_HAS_VERCEL_WORKERS = importlib.util.find_spec("vercel.workers") is not None


def pytest_ignore_collect(collection_path: Path, config: pytest.Config) -> bool | None:
    if not _HAS_VERCEL_WORKERS and collection_path.name.startswith("test_workflow_"):
        return True
    return None
