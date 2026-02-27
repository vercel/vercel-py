"""Tests for HTTP URL construction using httpx base_url.

These tests verify that the transport layer normalizes URLs consistently:
- base_url is always normalized to end with a trailing slash
- path is always normalized to not start with a leading slash
- This ensures consistent URL joining: final_url = base_url + path

Users can pass base_url with or without trailing slash, and paths with or
without leading slash - the result will be the same.
"""

import pytest
import respx
from httpx import Response

from vercel._internal.http import (
    AsyncTransport,
    JSONBody,
    SyncTransport,
    create_base_async_client,
    create_base_client,
)
from vercel._internal.iter_coroutine import iter_coroutine


class TestUrlNormalization:
    """Test that URL normalization produces consistent results regardless of input format."""

    @pytest.mark.parametrize(
        "base_url,path,expected_url",
        [
            # All four combinations of trailing/leading slash produce the same result
            ("https://api.example.com", "/v1/projects", "https://api.example.com/v1/projects"),
            ("https://api.example.com/", "/v1/projects", "https://api.example.com/v1/projects"),
            ("https://api.example.com", "v1/projects", "https://api.example.com/v1/projects"),
            ("https://api.example.com/", "v1/projects", "https://api.example.com/v1/projects"),
            # Same for paths with multiple segments
            ("https://api.example.com", "/v1/cache/keys", "https://api.example.com/v1/cache/keys"),
            ("https://api.example.com/", "v1/cache/keys", "https://api.example.com/v1/cache/keys"),
            # Base URL with path segment - all variants work
            ("https://api.example.com/v1", "/projects", "https://api.example.com/v1/projects"),
            ("https://api.example.com/v1/", "/projects", "https://api.example.com/v1/projects"),
            ("https://api.example.com/v1", "projects", "https://api.example.com/v1/projects"),
            ("https://api.example.com/v1/", "projects", "https://api.example.com/v1/projects"),
        ],
        ids=[
            "no_trailing_with_leading",
            "trailing_with_leading",
            "no_trailing_no_leading",
            "trailing_no_leading",
            "multi_segment_no_trailing_with_leading",
            "multi_segment_trailing_no_leading",
            "base_with_path_no_trailing_with_leading",
            "base_with_path_trailing_with_leading",
            "base_with_path_no_trailing_no_leading",
            "base_with_path_trailing_no_leading",
        ],
    )
    @respx.mock
    def test_sync_normalization_consistency(self, base_url: str, path: str, expected_url: str):
        """Test that SyncTransport normalizes URLs consistently."""
        route = respx.get(expected_url).mock(return_value=Response(200, json={"ok": True}))

        client = create_base_client(timeout=30.0, base_url=base_url)
        transport = SyncTransport(client)

        try:
            response = iter_coroutine(transport.send("GET", path))
            assert response.status_code == 200
            assert route.called
        finally:
            transport.close()

    @pytest.mark.parametrize(
        "base_url,path,expected_url",
        [
            # All four combinations produce the same result
            ("https://api.example.com", "/v1/projects", "https://api.example.com/v1/projects"),
            ("https://api.example.com/", "/v1/projects", "https://api.example.com/v1/projects"),
            ("https://api.example.com", "v1/projects", "https://api.example.com/v1/projects"),
            ("https://api.example.com/", "v1/projects", "https://api.example.com/v1/projects"),
        ],
        ids=[
            "no_trailing_with_leading",
            "trailing_with_leading",
            "no_trailing_no_leading",
            "trailing_no_leading",
        ],
    )
    @respx.mock
    @pytest.mark.asyncio
    async def test_async_normalization_consistency(
        self, base_url: str, path: str, expected_url: str
    ):
        """Test that AsyncTransport normalizes URLs consistently."""
        route = respx.get(expected_url).mock(return_value=Response(200, json={"ok": True}))

        client = create_base_async_client(timeout=30.0, base_url=base_url)
        transport = AsyncTransport(client)

        try:
            response = await transport.send("GET", path)
            assert response.status_code == 200
            assert route.called
        finally:
            await transport.aclose()


class TestEdgeCases:
    """Test edge cases for URL construction."""

    @pytest.mark.parametrize(
        "base_url,path,expected_url",
        [
            # Empty path
            ("https://api.example.com/", "", "https://api.example.com/"),
            ("https://api.example.com", "", "https://api.example.com/"),
            # Root path
            ("https://api.example.com/", "/", "https://api.example.com/"),
            ("https://api.example.com", "/", "https://api.example.com/"),
            # Nested base URL
            (
                "https://api.example.com/v1/cache/",
                "my-key",
                "https://api.example.com/v1/cache/my-key",
            ),
        ],
        ids=[
            "empty_path_trailing_base",
            "empty_path_no_trailing_base",
            "root_path_trailing_base",
            "root_path_no_trailing_base",
            "nested_base_url",
        ],
    )
    @respx.mock
    def test_edge_cases(self, base_url: str, path: str, expected_url: str):
        """Test edge cases for URL construction."""
        route = respx.get(expected_url).mock(return_value=Response(200, json={"ok": True}))

        client = create_base_client(timeout=30.0, base_url=base_url)
        transport = SyncTransport(client)

        try:
            response = iter_coroutine(transport.send("GET", path))
            assert response.status_code == 200
            assert route.called
        finally:
            transport.close()


class TestCacheUrlPatterns:
    """Test URL patterns specifically used by the cache module.

    The cache module uses paths without leading slashes (e.g., cache keys like
    "my-key" and actions like "revalidate"). These tests verify that pattern
    works correctly with base_url ending in a trailing slash.
    """

    @respx.mock
    def test_cache_get_key(self):
        """Test GET request for a cache key."""
        base_url = "https://cache.example.com/v1/"
        key = "user-123-profile"
        expected = f"{base_url}{key}"

        route = respx.get(expected).mock(return_value=Response(200, json={"data": "cached"}))

        client = create_base_client(timeout=30.0, base_url=base_url)
        transport = SyncTransport(client)

        try:
            response = iter_coroutine(transport.send("GET", key))
            assert response.status_code == 200
            assert route.called
        finally:
            transport.close()

    @respx.mock
    def test_cache_set_key(self):
        """Test POST request to set a cache key."""
        base_url = "https://cache.example.com/v1/"
        key = "user-456-settings"
        expected = f"{base_url}{key}"

        route = respx.post(expected).mock(return_value=Response(200, json={"ok": True}))

        client = create_base_client(timeout=30.0, base_url=base_url)
        transport = SyncTransport(client)

        try:
            response = iter_coroutine(transport.send("POST", key, body=JSONBody({"value": "test"})))
            assert response.status_code == 200
            assert route.called
        finally:
            transport.close()

    @respx.mock
    def test_cache_revalidate(self):
        """Test POST request to revalidate endpoint."""
        base_url = "https://cache.example.com/v1/"
        expected = f"{base_url}revalidate"

        route = respx.post(expected).mock(return_value=Response(200, json={"ok": True}))

        client = create_base_client(timeout=30.0, base_url=base_url)
        transport = SyncTransport(client)

        try:
            response = iter_coroutine(transport.send("POST", "revalidate", params={"tags": "foo"}))
            assert response.status_code == 200
            assert route.called
        finally:
            transport.close()


class TestApiUrlPatterns:
    """Test URL patterns used by the API clients (projects, deployments, etc.).

    The API clients use paths with leading slashes (e.g., "/v10/projects").
    """

    @respx.mock
    def test_projects_list(self):
        """Test GET request for projects list."""
        base_url = "https://api.vercel.com"
        path = "/v10/projects"
        expected = f"{base_url}{path}"

        route = respx.get(expected).mock(return_value=Response(200, json={"projects": []}))

        client = create_base_client(timeout=30.0, base_url=base_url)
        transport = SyncTransport(client)

        try:
            response = iter_coroutine(transport.send("GET", path))
            assert response.status_code == 200
            assert route.called
        finally:
            transport.close()

    @respx.mock
    def test_project_by_id(self):
        """Test GET request for a specific project."""
        base_url = "https://api.vercel.com"
        project_id = "prj_abc123"
        path = f"/v9/projects/{project_id}"
        expected = f"{base_url}{path}"

        route = respx.get(expected).mock(return_value=Response(200, json={"id": project_id}))

        client = create_base_client(timeout=30.0, base_url=base_url)
        transport = SyncTransport(client)

        try:
            response = iter_coroutine(transport.send("GET", path))
            assert response.status_code == 200
            assert route.called
        finally:
            transport.close()
