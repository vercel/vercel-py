"""
Unit tests for client class instantiation.

These tests verify that client classes can be instantiated without errors.
This catches issues like passing invalid kwargs to dataclasses (e.g., HTTPConfig).
"""

import os
from unittest.mock import patch

import pytest


class TestClientInstantiation:
    """Test that all client classes can be instantiated."""

    @pytest.fixture
    def mock_env_token(self):
        """Provide a mock token via environment variable."""
        with patch.dict(os.environ, {"VERCEL_TOKEN": "test_token"}):
            yield

    def test_projects_client_instantiation(self, mock_env_token):
        """Test ProjectsClient can be instantiated."""
        from vercel.projects.client import ProjectsClient

        client = ProjectsClient()
        assert client is not None
        assert hasattr(client, "_transport")
        assert hasattr(client, "_token")

    def test_projects_client_with_token(self):
        """Test ProjectsClient can be instantiated with explicit token."""
        from vercel.projects.client import ProjectsClient

        client = ProjectsClient(access_token="explicit_token")
        assert client is not None
        assert client._token == "explicit_token"

    def test_async_projects_client_instantiation(self, mock_env_token):
        """Test AsyncProjectsClient can be instantiated."""
        from vercel.projects.client import AsyncProjectsClient

        client = AsyncProjectsClient()
        assert client is not None
        assert hasattr(client, "_transport")
        assert hasattr(client, "_token")

    def test_deployments_client_instantiation(self, mock_env_token):
        """Test DeploymentsClient can be instantiated."""
        from vercel.deployments.client import DeploymentsClient

        client = DeploymentsClient()
        assert client is not None
        assert hasattr(client, "_transport")
        assert hasattr(client, "_token")

    def test_deployments_client_with_token(self):
        """Test DeploymentsClient can be instantiated with explicit token."""
        from vercel.deployments.client import DeploymentsClient

        client = DeploymentsClient(access_token="explicit_token")
        assert client is not None
        assert client._token == "explicit_token"

    def test_async_deployments_client_instantiation(self, mock_env_token):
        """Test AsyncDeploymentsClient can be instantiated."""
        from vercel.deployments.client import AsyncDeploymentsClient

        client = AsyncDeploymentsClient()
        assert client is not None
        assert hasattr(client, "_transport")
        assert hasattr(client, "_token")

    def test_oidc_client_instantiation(self):
        """Test SyncOidcClient can be instantiated (no token required)."""
        from vercel.oidc._core import SyncOidcClient

        client = SyncOidcClient()
        assert client is not None
        assert hasattr(client, "_transport")

    def test_async_oidc_client_instantiation(self):
        """Test AsyncOidcClient can be instantiated (no token required)."""
        from vercel.oidc._core import AsyncOidcClient

        client = AsyncOidcClient()
        assert client is not None
        assert hasattr(client, "_transport")

    def test_cache_client_instantiation(self):
        """Test SyncBuildCache can be instantiated."""
        from vercel.cache._core import SyncBuildCache

        client = SyncBuildCache(
            endpoint="https://cache.example.com",
            headers={"Authorization": "Bearer test"},
        )
        assert client is not None
        assert hasattr(client, "_transport")

    def test_async_cache_client_instantiation(self):
        """Test AsyncBuildCache can be instantiated."""
        from vercel.cache._core import AsyncBuildCache

        client = AsyncBuildCache(
            endpoint="https://cache.example.com",
            headers={"Authorization": "Bearer test"},
        )
        assert client is not None
        assert hasattr(client, "_transport")
