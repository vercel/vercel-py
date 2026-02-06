"""Integration tests for Vercel Blob API using respx mocking.

Tests both sync and async variants to ensure API parity.
"""

import httpx
import pytest
import respx

from vercel.blob import (
    AsyncBlobClient,
    BlobClient,
    BlobNotFoundError,
    copy,
    copy_async,
    create_folder,
    create_folder_async,
    delete,
    delete_async,
    head,
    head_async,
    iter_objects,
    iter_objects_async,
    list_objects,
    list_objects_async,
    put,
    put_async,
)

# Base URL for Vercel Blob API
BLOB_API_BASE = "https://vercel.com/api/blob"


class TestBlobPut:
    """Test blob put operations."""

    @respx.mock
    def test_put_sync(self, mock_env_clear, mock_blob_put_response):
        """Test synchronous blob upload."""
        route = respx.put(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_put_response)
        )

        result = put("test.txt", b"Hello, World!", token="test_token")

        assert route.called
        assert result.url == mock_blob_put_response["url"]
        assert result.pathname == "test.txt"
        assert result.content_type == "text/plain"

        # Verify request had correct headers
        request = route.calls.last.request
        assert "Bearer test_token" in request.headers.get("authorization", "")

    @respx.mock
    @pytest.mark.asyncio
    async def test_put_async(self, mock_env_clear, mock_blob_put_response):
        """Test asynchronous blob upload."""
        route = respx.put(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_put_response)
        )

        result = await put_async("test.txt", b"Hello, World!", token="test_token")

        assert route.called
        assert result.url == mock_blob_put_response["url"]
        assert result.pathname == "test.txt"

    @respx.mock
    def test_put_with_content_type(self, mock_env_clear, mock_blob_put_response):
        """Test put with explicit content type."""
        route = respx.put(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_put_response)
        )

        result = put(
            "test.json",
            b'{"key": "value"}',
            token="test_token",
            content_type="application/json",
        )

        assert route.called
        request = route.calls.last.request
        assert request.headers.get("x-content-type") == "application/json"
        # Verify result is properly parsed
        assert result.url == mock_blob_put_response["url"]
        assert result.content_type == mock_blob_put_response["contentType"]

    @respx.mock
    def test_put_with_cache_control(self, mock_env_clear, mock_blob_put_response):
        """Test put with cache control max age."""
        route = respx.put(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_put_response)
        )

        put("test.txt", b"data", token="test_token", cache_control_max_age=3600)

        assert route.called
        request = route.calls.last.request
        assert request.headers.get("x-cache-control-max-age") == "3600"

    @respx.mock
    def test_put_sync_async_parity(self, mock_env_clear, mock_blob_put_response):
        """Verify sync and async produce identical results."""
        respx.put(BLOB_API_BASE).mock(return_value=httpx.Response(200, json=mock_blob_put_response))

        sync_result = put("test.txt", b"data", token="test_token")

        # Reset mock for async call
        respx.reset()
        respx.put(BLOB_API_BASE).mock(return_value=httpx.Response(200, json=mock_blob_put_response))

        import asyncio

        async_result = asyncio.get_event_loop().run_until_complete(
            put_async("test.txt", b"data", token="test_token")
        )

        assert sync_result.url == async_result.url
        assert sync_result.pathname == async_result.pathname
        assert sync_result.content_type == async_result.content_type


class TestBlobDelete:
    """Test blob delete operations."""

    @respx.mock
    def test_delete_single_sync(self, mock_env_clear):
        """Test synchronous single blob delete."""
        route = respx.post(f"{BLOB_API_BASE}/delete").mock(
            return_value=httpx.Response(200, json={})
        )

        delete("https://blob.vercel-storage.com/test.txt", token="test_token")

        assert route.called
        request = route.calls.last.request
        # Verify the URL was sent in the request body
        import json

        body = json.loads(request.content)
        assert "urls" in body
        assert "https://blob.vercel-storage.com/test.txt" in body["urls"]

    @respx.mock
    @pytest.mark.asyncio
    async def test_delete_single_async(self, mock_env_clear):
        """Test asynchronous single blob delete."""
        route = respx.post(f"{BLOB_API_BASE}/delete").mock(
            return_value=httpx.Response(200, json={})
        )

        await delete_async("https://blob.vercel-storage.com/test.txt", token="test_token")

        assert route.called

    @respx.mock
    def test_delete_batch_sync(self, mock_env_clear):
        """Test synchronous batch blob delete."""
        route = respx.post(f"{BLOB_API_BASE}/delete").mock(
            return_value=httpx.Response(200, json={})
        )

        urls = [
            "https://blob.vercel-storage.com/file1.txt",
            "https://blob.vercel-storage.com/file2.txt",
            "https://blob.vercel-storage.com/file3.txt",
        ]
        delete(urls, token="test_token")

        assert route.called
        import json

        body = json.loads(route.calls.last.request.content)
        assert len(body["urls"]) == 3

    @respx.mock
    @pytest.mark.asyncio
    async def test_delete_batch_async(self, mock_env_clear):
        """Test asynchronous batch blob delete."""
        route = respx.post(f"{BLOB_API_BASE}/delete").mock(
            return_value=httpx.Response(200, json={})
        )

        urls = [
            "https://blob.vercel-storage.com/file1.txt",
            "https://blob.vercel-storage.com/file2.txt",
        ]
        await delete_async(urls, token="test_token")

        assert route.called


class TestBlobHead:
    """Test blob head/metadata operations."""

    @respx.mock
    def test_head_sync(self, mock_env_clear, mock_blob_head_response):
        """Test synchronous blob metadata retrieval."""
        route = respx.get(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_head_response)
        )

        result = head("https://blob.vercel-storage.com/test.txt", token="test_token")

        assert route.called
        assert result.size == 13
        assert result.pathname == "test.txt"
        assert result.content_type == "text/plain"

    @respx.mock
    @pytest.mark.asyncio
    async def test_head_async(self, mock_env_clear, mock_blob_head_response):
        """Test asynchronous blob metadata retrieval."""
        route = respx.get(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_head_response)
        )

        result = await head_async("https://blob.vercel-storage.com/test.txt", token="test_token")

        assert route.called
        assert result.size == 13
        assert result.pathname == "test.txt"

    @respx.mock
    def test_head_not_found(self, mock_env_clear, mock_error_not_found):
        """Test 404 error handling for head operation."""
        respx.get(BLOB_API_BASE).mock(return_value=httpx.Response(404, json=mock_error_not_found))

        with pytest.raises(BlobNotFoundError):
            head("https://blob.vercel-storage.com/nonexistent.txt", token="test_token")

    @respx.mock
    @pytest.mark.asyncio
    async def test_head_not_found_async(self, mock_env_clear, mock_error_not_found):
        """Test 404 error handling for async head operation."""
        respx.get(BLOB_API_BASE).mock(return_value=httpx.Response(404, json=mock_error_not_found))

        with pytest.raises(BlobNotFoundError):
            await head_async("https://blob.vercel-storage.com/nonexistent.txt", token="test_token")


class TestBlobList:
    """Test blob list operations."""

    @respx.mock
    def test_list_objects_sync(self, mock_env_clear, mock_blob_list_response):
        """Test synchronous blob listing."""
        route = respx.get(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_list_response)
        )

        result = list_objects(token="test_token")

        assert route.called
        assert len(result.blobs) == 2
        assert result.blobs[0].pathname == "file1.txt"
        assert result.blobs[1].pathname == "file2.txt"
        assert result.has_more is False

    @respx.mock
    @pytest.mark.asyncio
    async def test_list_objects_async(self, mock_env_clear, mock_blob_list_response):
        """Test asynchronous blob listing."""
        route = respx.get(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_list_response)
        )

        result = await list_objects_async(token="test_token")

        assert route.called
        assert len(result.blobs) == 2

    @respx.mock
    def test_list_objects_with_prefix(self, mock_env_clear, mock_blob_list_response):
        """Test list with prefix filter."""
        route = respx.get(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_list_response)
        )

        list_objects(prefix="files/", token="test_token")

        assert route.called
        request = route.calls.last.request
        assert "prefix=files%2F" in str(request.url) or "prefix=files/" in str(request.url)

    @respx.mock
    def test_list_objects_with_limit(self, mock_env_clear, mock_blob_list_response):
        """Test list with limit parameter."""
        route = respx.get(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_list_response)
        )

        list_objects(limit=10, token="test_token")

        assert route.called
        request = route.calls.last.request
        assert "limit=10" in str(request.url)

    @respx.mock
    def test_list_objects_pagination(self, mock_env_clear, mock_blob_list_response_paginated):
        """Test pagination with cursor."""
        route = respx.get(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_list_response_paginated)
        )

        result = list_objects(token="test_token")

        assert route.called
        assert result.has_more is True
        assert result.cursor == "next_cursor_abc123"


class TestBlobIterObjects:
    """Test blob iteration operations."""

    @respx.mock
    def test_iter_objects_sync(self, mock_env_clear, mock_blob_list_response):
        """Test synchronous blob iteration."""
        respx.get(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_list_response)
        )

        items = list(iter_objects(token="test_token"))

        assert len(items) == 2
        assert items[0].pathname == "file1.txt"
        assert items[1].pathname == "file2.txt"

    @respx.mock
    @pytest.mark.asyncio
    async def test_iter_objects_async(self, mock_env_clear, mock_blob_list_response):
        """Test asynchronous blob iteration."""
        respx.get(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_list_response)
        )

        items = []
        async for item in iter_objects_async(token="test_token"):
            items.append(item)

        assert len(items) == 2

    @respx.mock
    def test_iter_objects_with_limit(self, mock_env_clear, mock_blob_list_response):
        """Test iteration with limit."""
        respx.get(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_list_response)
        )

        items = list(iter_objects(limit=1, token="test_token"))

        assert len(items) == 1

    @respx.mock
    def test_iter_objects_pagination(
        self, mock_env_clear, mock_blob_list_response_paginated, mock_blob_list_response
    ):
        """Test iteration across multiple pages."""
        # First call returns paginated response
        respx.get(BLOB_API_BASE).mock(
            side_effect=[
                httpx.Response(200, json=mock_blob_list_response_paginated),
                httpx.Response(200, json=mock_blob_list_response),
            ]
        )

        items = list(iter_objects(token="test_token"))

        # Should have items from both pages (1 + 2 = 3)
        assert len(items) == 3


class TestBlobCopy:
    """Test blob copy operations."""

    @respx.mock
    def test_copy_sync(self, mock_env_clear, mock_blob_copy_response):
        """Test synchronous blob copy."""
        route = respx.put(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_copy_response)
        )

        result = copy(
            "https://blob.vercel-storage.com/source.txt",
            "copied.txt",
            token="test_token",
        )

        assert route.called
        assert result.pathname == "copied.txt"

        # Verify fromUrl parameter was sent
        request = route.calls.last.request
        assert "fromUrl" in str(request.url)

    @respx.mock
    @pytest.mark.asyncio
    async def test_copy_async(self, mock_env_clear, mock_blob_copy_response):
        """Test asynchronous blob copy."""
        route = respx.put(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_copy_response)
        )

        result = await copy_async(
            "https://blob.vercel-storage.com/source.txt",
            "copied.txt",
            token="test_token",
        )

        assert route.called
        assert result.pathname == "copied.txt"


class TestBlobCreateFolder:
    """Test blob folder creation."""

    @respx.mock
    def test_create_folder_sync(self, mock_env_clear, mock_blob_create_folder_response):
        """Test synchronous folder creation."""
        route = respx.put(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_create_folder_response)
        )

        result = create_folder("my-folder", token="test_token")

        assert route.called
        assert result.pathname == "my-folder/"

        # Verify pathname ends with /
        request = route.calls.last.request
        assert "pathname=my-folder%2F" in str(request.url) or "pathname=my-folder/" in str(
            request.url
        )

    @respx.mock
    @pytest.mark.asyncio
    async def test_create_folder_async(self, mock_env_clear, mock_blob_create_folder_response):
        """Test asynchronous folder creation."""
        route = respx.put(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_create_folder_response)
        )

        result = await create_folder_async("my-folder", token="test_token")

        assert route.called
        assert result.pathname == "my-folder/"


class TestBlobClient:
    """Test BlobClient and AsyncBlobClient classes."""

    @respx.mock
    def test_blob_client_put(self, mock_env_clear, mock_blob_put_response):
        """Test BlobClient put method."""
        route = respx.put(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_put_response)
        )

        client = BlobClient(token="test_token")
        result = client.put("test.txt", b"Hello, World!")

        assert route.called
        assert result.url == mock_blob_put_response["url"]

    @respx.mock
    def test_blob_client_head(self, mock_env_clear, mock_blob_head_response):
        """Test BlobClient head method."""
        route = respx.get(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_head_response)
        )

        client = BlobClient(token="test_token")
        result = client.head("https://blob.vercel-storage.com/test.txt")

        assert route.called
        assert result.size == 13

    @respx.mock
    def test_blob_client_delete(self, mock_env_clear):
        """Test BlobClient delete method."""
        route = respx.post(f"{BLOB_API_BASE}/delete").mock(
            return_value=httpx.Response(200, json={})
        )

        client = BlobClient(token="test_token")
        client.delete("https://blob.vercel-storage.com/test.txt")

        assert route.called

    @respx.mock
    def test_blob_client_list_objects(self, mock_env_clear, mock_blob_list_response):
        """Test BlobClient list_objects method."""
        route = respx.get(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_list_response)
        )

        client = BlobClient(token="test_token")
        result = client.list_objects()

        assert route.called
        assert len(result.blobs) == 2

    @respx.mock
    @pytest.mark.asyncio
    async def test_async_blob_client_put(self, mock_env_clear, mock_blob_put_response):
        """Test AsyncBlobClient put method."""
        route = respx.put(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_put_response)
        )

        client = AsyncBlobClient(token="test_token")
        result = await client.put("test.txt", b"Hello, World!")

        assert route.called
        assert result.url == mock_blob_put_response["url"]

    @respx.mock
    @pytest.mark.asyncio
    async def test_async_blob_client_head(self, mock_env_clear, mock_blob_head_response):
        """Test AsyncBlobClient head method."""
        route = respx.get(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_head_response)
        )

        client = AsyncBlobClient(token="test_token")
        result = await client.head("https://blob.vercel-storage.com/test.txt")

        assert route.called
        assert result.size == 13


class TestBlobErrorHandling:
    """Test error handling for blob operations."""

    @respx.mock
    def test_missing_token_raises_error(self, mock_env_clear):
        """Test that missing token raises BlobError."""
        from vercel.blob.errors import BlobError

        # Don't mock any routes - we expect failure before HTTP call
        with pytest.raises(BlobError):
            put("test.txt", b"data")

    @respx.mock
    def test_not_found_error(self, mock_env_clear, mock_error_not_found):
        """Test BlobNotFoundError is raised on 404."""
        respx.get(BLOB_API_BASE).mock(return_value=httpx.Response(404, json=mock_error_not_found))

        with pytest.raises(BlobNotFoundError):
            head("https://blob.vercel-storage.com/missing.txt", token="test_token")

    @respx.mock
    def test_access_error(self, mock_env_clear, mock_error_forbidden):
        """Test BlobAccessError is raised on 403."""
        from vercel.blob import BlobAccessError

        respx.get(BLOB_API_BASE).mock(return_value=httpx.Response(403, json=mock_error_forbidden))

        with pytest.raises(BlobAccessError):
            head("https://blob.vercel-storage.com/forbidden.txt", token="test_token")

    @respx.mock
    @pytest.mark.asyncio
    async def test_not_found_error_async(self, mock_env_clear, mock_error_not_found):
        """Test BlobNotFoundError is raised on 404 for async."""
        respx.get(BLOB_API_BASE).mock(return_value=httpx.Response(404, json=mock_error_not_found))

        with pytest.raises(BlobNotFoundError):
            await head_async("https://blob.vercel-storage.com/missing.txt", token="test_token")
