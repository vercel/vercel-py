"""
E2E Test Configuration and Environment Setup

This module provides configuration and utilities for e2e tests.
"""

import pytest
from typing import Any, Optional

from tests.e2e.config import E2ETestConfig


def skip_if_missing_token(token_name: str, token_value: Any) -> None:
    """Skip test if required token is missing."""
    if not token_value:
        pytest.skip(f"{token_name} not set - skipping test")


def skip_if_missing_tokens(**tokens) -> None:
    """Skip test if any required tokens are missing."""
    missing = [name for name, value in tokens.items() if not value]
    if missing:
        pytest.skip(f"Missing required tokens: {', '.join(missing)}")


class E2ETestBase:
    """Base class for E2E tests with common utilities."""

    def __init__(self):
        self.config = E2ETestConfig()
        self.test_prefix = self.config.get_test_prefix()
        self.uploaded_blobs = []
        self.created_projects = []

    def cleanup_blobs(self, blob_token: Optional[str]) -> None:
        """Clean up uploaded blobs."""
        if blob_token and self.uploaded_blobs:
            import asyncio
            from vercel import blob

            async def cleanup():
                try:
                    await blob.delete(self.uploaded_blobs, token=blob_token)
                except Exception:
                    # Some blobs might already be deleted
                    pass

            asyncio.run(cleanup())

    def cleanup_projects(self, vercel_token: Optional[str], team_id: Optional[str]) -> None:
        """Clean up created projects."""
        if vercel_token and self.created_projects:
            import asyncio
            from vercel.projects import delete_project

            async def cleanup():
                for project_id in self.created_projects:
                    try:
                        await delete_project(
                            project_id=project_id, token=vercel_token, team_id=team_id
                        )
                    except Exception:
                        # Project might already be deleted
                        pass

            asyncio.run(cleanup())

    def cleanup_cache(self, namespace: str) -> None:
        """Clean up cache entries."""
        import asyncio
        from vercel.cache import get_cache

        async def cleanup():
            cache = get_cache(namespace=namespace)
            await cache.expire_tag("test")
            await cache.expire_tag("e2e")
            await cache.expire_tag("integration")

        asyncio.run(cleanup())


# Pytest fixtures for common test setup
@pytest.fixture
def e2e_config():
    """Get E2E test configuration."""
    return E2ETestConfig()


@pytest.fixture
def e2e_test_base():
    """Get E2E test base instance."""
    return E2ETestBase()


@pytest.fixture
def test_prefix():
    """Get a unique test prefix."""
    return E2ETestConfig.get_test_prefix()


# Skip decorators for conditional tests
def skip_if_no_blob_token(func):
    """Skip test if blob token is not available."""

    def wrapper(*args, **kwargs):
        if not E2ETestConfig.is_blob_enabled():
            pytest.skip("BLOB_READ_WRITE_TOKEN not set")
        return func(*args, **kwargs)

    return wrapper


def skip_if_no_vercel_token(func):
    """Skip test if Vercel token is not available."""

    def wrapper(*args, **kwargs):
        if not E2ETestConfig.is_vercel_api_enabled():
            pytest.skip("VERCEL_TOKEN not set")
        return func(*args, **kwargs)

    return wrapper


def skip_if_no_oidc_token(func):
    """Skip test if OIDC token is not available."""

    def wrapper(*args, **kwargs):
        if not E2ETestConfig.is_oidc_enabled():
            pytest.skip("VERCEL_OIDC_TOKEN not set")
        return func(*args, **kwargs)

    return wrapper
