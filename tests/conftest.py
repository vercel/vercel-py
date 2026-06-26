"""Shared fixtures for all tests."""

import importlib.util
import os
import time
import uuid
from collections.abc import Generator
from pathlib import Path
from typing import Any

import httpcore2
import httpx2
import pytest
import respx
import respx.mocks
import respx.models
import respx.router
from respx.mocks import HTTPCoreMocker


class HTTPCore2Mocker(HTTPCoreMocker):
    name = "httpcore2"
    targets = [
        "httpcore2._sync.connection.HTTPConnection",
        "httpcore2._sync.connection_pool.ConnectionPool",
        "httpcore2._sync.http_proxy.HTTPProxy",
        "httpcore2._async.connection.AsyncHTTPConnection",
        "httpcore2._async.connection_pool.AsyncConnectionPool",
        "httpcore2._async.http_proxy.AsyncHTTPProxy",
    ]

    @classmethod
    def to_httpx_request(cls, **kwargs: Any) -> httpx2.Request:
        request = kwargs["request"]
        method = (
            request.method.decode("ascii")
            if isinstance(request.method, bytes)
            else request.method
        )
        scheme = (
            request.url.scheme.decode("ascii")
            if isinstance(request.url.scheme, bytes)
            else request.url.scheme
        )
        host = (
            request.url.host.decode("ascii")
            if isinstance(request.url.host, bytes)
            else request.url.host
        )
        return httpx2.Request(
            method,
            httpx2.URL(
                scheme=scheme,
                host=host,
                port=request.url.port,
                raw_path=request.url.target,
            ),
            headers=request.headers,
            stream=request.stream,
            extensions=request.extensions,
        )

    @classmethod
    def from_sync_httpx_response(
        cls, httpx_response: httpx2.Response, target: object, **kwargs: Any
    ) -> httpcore2.Response:
        return httpcore2.Response(
            status=httpx_response.status_code,
            headers=httpx_response.headers.raw,
            content=httpx_response.stream,
            extensions=httpx_response.extensions,
        )


respx.models.httpx = httpx2
respx.mocks.httpx = httpx2
respx.router.httpx = httpx2
respx.mocks.DEFAULT_MOCKER = HTTPCore2Mocker.name


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

    set_headers(None)
    yield
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


def has_sandbox_credentials() -> bool:
    """Check if Sandbox credentials are available."""
    return bool(
        os.getenv("VERCEL_TOKEN") and os.getenv("VERCEL_TEAM_ID") and os.getenv("VERCEL_PROJECT_ID")
    )


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
    reason="Requires VERCEL_TOKEN, VERCEL_TEAM_ID, and VERCEL_PROJECT_ID for sandbox",
)


# Workflow tests import the workflow worlds, which depend on vercel-workers
# (installed only on Python >= 3.12; see pyproject). Skip collecting them when the
# package is unavailable, so collection doesn't error on older interpreters.
_HAS_VERCEL_WORKERS = importlib.util.find_spec("vercel.workers") is not None


def pytest_ignore_collect(collection_path: Path, config: pytest.Config) -> bool | None:
    if not _HAS_VERCEL_WORKERS and collection_path.name.startswith("test_workflow_"):
        return True
    return None
