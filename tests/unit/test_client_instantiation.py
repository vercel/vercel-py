"""
Unit tests for client class instantiation.

These tests verify that client classes can be instantiated without errors.
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
        assert hasattr(client, "_access_token")
        assert hasattr(client, "_base_url")
        assert hasattr(client, "_timeout")

    def test_projects_client_with_token(self):
        """Test ProjectsClient can be instantiated with explicit token."""
        from vercel.projects.client import ProjectsClient

        client = ProjectsClient(access_token="explicit_token")
        assert client is not None
        assert client._access_token == "explicit_token"

    def test_async_projects_client_instantiation(self, mock_env_token):
        """Test AsyncProjectsClient can be instantiated."""
        from vercel.projects.client import AsyncProjectsClient

        client = AsyncProjectsClient()
        assert client is not None
        assert hasattr(client, "_access_token")
        assert hasattr(client, "_base_url")
        assert hasattr(client, "_timeout")

    def test_deployments_client_instantiation(self, mock_env_token):
        """Test DeploymentsClient can be instantiated."""
        from vercel.deployments.client import DeploymentsClient

        client = DeploymentsClient()
        assert client is not None
        assert hasattr(client, "_access_token")
        assert hasattr(client, "_base_url")
        assert hasattr(client, "_timeout")

    def test_deployments_client_with_token(self):
        """Test DeploymentsClient can be instantiated with explicit token."""
        from vercel.deployments.client import DeploymentsClient

        client = DeploymentsClient(access_token="explicit_token")
        assert client is not None
        assert client._access_token == "explicit_token"

    def test_async_deployments_client_instantiation(self, mock_env_token):
        """Test AsyncDeploymentsClient can be instantiated."""
        from vercel.deployments.client import AsyncDeploymentsClient

        client = AsyncDeploymentsClient()
        assert client is not None
        assert hasattr(client, "_access_token")
        assert hasattr(client, "_base_url")
        assert hasattr(client, "_timeout")

    def test_build_cache_instantiation(self):
        """Test BuildCache can be instantiated."""
        from vercel.cache.cache_build import BuildCache

        client = BuildCache(
            endpoint="https://cache.example.com",
            headers={"Authorization": "Bearer test"},
        )
        assert client is not None
        assert hasattr(client, "_endpoint")
        assert hasattr(client, "_headers")
        assert hasattr(client, "_client")

    def test_async_build_cache_instantiation(self):
        """Test AsyncBuildCache can be instantiated."""
        from vercel.cache.cache_build import AsyncBuildCache

        client = AsyncBuildCache(
            endpoint="https://cache.example.com",
            headers={"Authorization": "Bearer test"},
        )
        assert client is not None
        assert hasattr(client, "_endpoint")
        assert hasattr(client, "_headers")
