"""
E2E Test Configuration and Environment Setup

This module provides configuration and utilities for e2e tests.
"""

import os
import pytest
from typing import Dict, Any, Optional


class E2ETestConfig:
    """Configuration for E2E tests."""
    
    # Environment variable names
    BLOB_TOKEN_ENV = 'BLOB_READ_WRITE_TOKEN'
    VERCEL_TOKEN_ENV = 'VERCEL_TOKEN'
    OIDC_TOKEN_ENV = 'VERCEL_OIDC_TOKEN'
    PROJECT_ID_ENV = 'VERCEL_PROJECT_ID'
    TEAM_ID_ENV = 'VERCEL_TEAM_ID'
    
    @classmethod
    def get_blob_token(cls) -> Optional[str]:
        """Get blob storage token."""
        return os.getenv(cls.BLOB_TOKEN_ENV)
    
    @classmethod
    def get_vercel_token(cls) -> Optional[str]:
        """Get Vercel API token."""
        return os.getenv(cls.VERCEL_TOKEN_ENV)
    
    @classmethod
    def get_oidc_token(cls) -> Optional[str]:
        """Get OIDC token."""
        return os.getenv(cls.OIDC_TOKEN_ENV)
    
    @classmethod
    def get_project_id(cls) -> Optional[str]:
        """Get Vercel project ID."""
        return os.getenv(cls.PROJECT_ID_ENV)
    
    @classmethod
    def get_team_id(cls) -> Optional[str]:
        """Get Vercel team ID."""
        return os.getenv(cls.TEAM_ID_ENV)
    
    @classmethod
    def is_blob_enabled(cls) -> bool:
        """Check if blob storage is enabled."""
        return cls.get_blob_token() is not None
    
    @classmethod
    def is_vercel_api_enabled(cls) -> bool:
        """Check if Vercel API is enabled."""
        return cls.get_vercel_token() is not None
    
    @classmethod
    def is_oidc_enabled(cls) -> bool:
        """Check if OIDC is enabled."""
        return cls.get_oidc_token() is not None
    
    @classmethod
    def get_test_prefix(cls) -> str:
        """Get a unique test prefix."""
        import time
        return f"e2e-test-{int(time.time())}"
    
    @classmethod
    def get_required_env_vars(cls) -> Dict[str, str]:
        """Get all required environment variables."""
        return {
            cls.BLOB_TOKEN_ENV: cls.get_blob_token(),
            cls.VERCEL_TOKEN_ENV: cls.get_vercel_token(),
            cls.OIDC_TOKEN_ENV: cls.get_oidc_token(),
            cls.PROJECT_ID_ENV: cls.get_project_id(),
            cls.TEAM_ID_ENV: cls.get_team_id(),
        }
    
    @classmethod
    def print_env_status(cls) -> None:
        """Print the status of environment variables."""
        print("E2E Test Environment Status:")
        print("=" * 40)
        
        env_vars = cls.get_required_env_vars()
        for env_var, value in env_vars.items():
            status = "✓" if value else "✗"
            print(f"{status} {env_var}: {'Set' if value else 'Not set'}")
        
        print("=" * 40)


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
                            project_id=project_id,
                            token=vercel_token,
                            team_id=team_id
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
