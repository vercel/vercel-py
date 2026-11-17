"""Shared fixtures for all tests."""

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
        "VERCEL_OIDC_TOKEN_FILE",
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
