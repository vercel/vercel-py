"""
E2E Test Configuration

This module provides configuration for e2e tests without pytest dependency.
"""

import os
from typing import Dict, Optional


class E2ETestConfig:
    """Configuration for E2E tests."""

    # Environment variable names
    BLOB_TOKEN_ENV = "BLOB_READ_WRITE_TOKEN"
    VERCEL_TOKEN_ENV = "VERCEL_TOKEN"
    OIDC_TOKEN_ENV = "VERCEL_OIDC_TOKEN"
    PROJECT_ID_ENV = "VERCEL_PROJECT_ID"
    TEAM_ID_ENV = "VERCEL_TEAM_ID"

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

        # Special note for OIDC token
        oidc_token = cls.get_oidc_token()
        vercel_token = cls.get_vercel_token()
        if oidc_token:
            print("✅ OIDC Token: Available - Tests will use full OIDC validation")
        elif vercel_token:
            print("⚠️  OIDC Token: Not available - Tests will use Vercel API token fallback")
        else:
            print("❌ OIDC Token: Not available - OIDC tests will be skipped")

        print("=" * 40)
