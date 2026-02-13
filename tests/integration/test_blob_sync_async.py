"""Integration tests for Vercel Blob API using respx mocking.

Tests both sync and async variants to ensure API parity.
"""

import io

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
    download_file,
    download_file_async,
    head,
    head_async,
    iter_objects,
    iter_objects_async,
    list_objects,
    list_objects_async,
    put,
    put_async,
)
from vercel.blob.ops import get, get_async

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
    def test_put_sync_progress_per_chunk(self, mock_env_clear, mock_blob_put_response):
        """Test sync put emits multiple progress callbacks for streamed file-like input."""
        payload = b"a" * (64 * 1024) + b"b" * 32
        progress_percentages: list[float] = []

        def handler(request: httpx.Request) -> httpx.Response:
            body = b"".join(request.stream)
            assert body == payload
            return httpx.Response(200, json=mock_blob_put_response)

        route = respx.put(BLOB_API_BASE).mock(side_effect=handler)

        put(
            "test.txt",
            io.BytesIO(payload),
            token="test_token",
            on_upload_progress=lambda event: progress_percentages.append(event.percentage),
        )

        assert route.called
        assert len(progress_percentages) >= 2
        assert progress_percentages[-1] == 100.0
        assert any(percentage < 100.0 for percentage in progress_percentages)

    @respx.mock
    @pytest.mark.asyncio
    async def test_put_async_progress_per_chunk(self, mock_env_clear, mock_blob_put_response):
        """Test async put emits multiple progress callbacks for streamed file-like input."""
        payload = b"a" * (64 * 1024) + b"b" * 32
        progress_percentages: list[float] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            body = b""
            async for chunk in request.stream:
                body += chunk
            assert body == payload
            return httpx.Response(200, json=mock_blob_put_response)

        route = respx.put(BLOB_API_BASE).mock(side_effect=handler)

        async def on_progress(event) -> None:
            progress_percentages.append(event.percentage)

        await put_async(
            "test.txt",
            io.BytesIO(payload),
            token="test_token",
            on_upload_progress=on_progress,
        )

        assert route.called
        assert len(progress_percentages) >= 2
        assert progress_percentages[-1] == 100.0
        assert any(percentage < 100.0 for percentage in progress_percentages)

    @respx.mock
    def test_put_sync_multipart_uses_runtime_upload(self, mock_env_clear):
        """Test sync multipart put uses create/upload/complete flow."""
        import json

        actions: list[str] = []
        completed_parts: list[dict[str, str | int]] = []

        def mpu_handler(request: httpx.Request) -> httpx.Response:
            action = request.headers["x-mpu-action"]
            actions.append(action)

            if action == "create":
                assert request.url.params["pathname"] == "folder/put-sync.bin"
                return httpx.Response(200, json={"uploadId": "upload-id", "key": "blob-key"})

            if action == "upload":
                assert request.headers["x-mpu-upload-id"] == "upload-id"
                assert request.headers["x-mpu-key"] == "blob-key"
                assert request.headers["x-mpu-part-number"] == "1"
                return httpx.Response(200, json={"etag": "etag-1"})

            if action == "complete":
                completed_parts.extend(json.loads(request.content.decode()))
                return httpx.Response(
                    200,
                    json={
                        "url": "https://blob.vercel-storage.com/test-abc123/folder/put-sync.bin",
                        "downloadUrl": (
                            "https://blob.vercel-storage.com/"
                            "test-abc123/folder/put-sync.bin?download=1"
                        ),
                        "pathname": "folder/put-sync.bin",
                        "contentType": "application/octet-stream",
                        "contentDisposition": 'inline; filename="put-sync.bin"',
                    },
                )

            raise AssertionError(f"unexpected multipart action: {action}")

        route = respx.post(f"{BLOB_API_BASE}/mpu").mock(side_effect=mpu_handler)

        result = put("folder/put-sync.bin", b"hello", token="test_token", multipart=True)

        assert route.call_count == 3
        assert actions == ["create", "upload", "complete"]
        assert [part["partNumber"] for part in completed_parts] == [1]
        assert result.pathname == "folder/put-sync.bin"

    @respx.mock
    @pytest.mark.asyncio
    async def test_put_async_multipart_uses_runtime_upload(self, mock_env_clear):
        """Test async multipart put uses create/upload/complete flow."""
        import json

        actions: list[str] = []
        completed_parts: list[dict[str, str | int]] = []

        def mpu_handler(request: httpx.Request) -> httpx.Response:
            action = request.headers["x-mpu-action"]
            actions.append(action)

            if action == "create":
                assert request.url.params["pathname"] == "folder/put-async.bin"
                return httpx.Response(200, json={"uploadId": "upload-id", "key": "blob-key"})

            if action == "upload":
                assert request.headers["x-mpu-upload-id"] == "upload-id"
                assert request.headers["x-mpu-key"] == "blob-key"
                assert request.headers["x-mpu-part-number"] == "1"
                return httpx.Response(200, json={"etag": "etag-1"})

            if action == "complete":
                completed_parts.extend(json.loads(request.content.decode()))
                return httpx.Response(
                    200,
                    json={
                        "url": "https://blob.vercel-storage.com/test-abc123/folder/put-async.bin",
                        "downloadUrl": (
                            "https://blob.vercel-storage.com/"
                            "test-abc123/folder/put-async.bin?download=1"
                        ),
                        "pathname": "folder/put-async.bin",
                        "contentType": "application/octet-stream",
                        "contentDisposition": 'inline; filename="put-async.bin"',
                    },
                )

            raise AssertionError(f"unexpected multipart action: {action}")

        route = respx.post(f"{BLOB_API_BASE}/mpu").mock(side_effect=mpu_handler)

        result = await put_async(
            "folder/put-async.bin",
            b"hello",
            token="test_token",
            multipart=True,
        )

        assert route.call_count == 3
        assert actions == ["create", "upload", "complete"]
        assert [part["partNumber"] for part in completed_parts] == [1]
        assert result.pathname == "folder/put-async.bin"

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
    def test_delete_batch_sync_generator(self, mock_env_clear):
        """Test synchronous delete with generator input preserves all URLs."""
        route = respx.post(f"{BLOB_API_BASE}/delete").mock(
            return_value=httpx.Response(200, json={})
        )

        urls = (f"https://blob.vercel-storage.com/file{i}.txt" for i in range(1, 4))
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

    @respx.mock
    @pytest.mark.asyncio
    async def test_delete_batch_async_generator(self, mock_env_clear):
        """Test asynchronous delete with generator input preserves all URLs."""
        route = respx.post(f"{BLOB_API_BASE}/delete").mock(
            return_value=httpx.Response(200, json={})
        )

        urls = (f"https://blob.vercel-storage.com/file{i}.txt" for i in range(1, 3))
        await delete_async(urls, token="test_token")

        assert route.called
        import json

        body = json.loads(route.calls.last.request.content)
        assert len(body["urls"]) == 2


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


class TestBlobReadAndDownload:
    """Test blob read and download operations."""

    @respx.mock
    def test_get_sync_with_url(self, mock_env_clear, mock_blob_head_response):
        """Test synchronous read from a direct blob URL."""
        payload = b"hello sync"
        route = respx.get(mock_blob_head_response["url"]).mock(
            return_value=httpx.Response(200, content=payload)
        )

        result = get(mock_blob_head_response["url"], token="test_token")

        assert route.called
        assert result == payload
        timeout = route.calls.last.request.extensions["timeout"]
        assert timeout["connect"] == 30.0

    @respx.mock
    @pytest.mark.asyncio
    async def test_get_async_with_path(self, mock_env_clear, mock_blob_head_response):
        """Test async read resolves path metadata before fetching bytes."""
        payload = b"hello async"
        head_route = respx.get(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_head_response)
        )
        blob_route = respx.get(mock_blob_head_response["url"]).mock(
            return_value=httpx.Response(200, content=payload)
        )

        result = await get_async("test.txt", token="test_token")

        assert head_route.called
        assert blob_route.called
        assert result == payload
        timeout = blob_route.calls.last.request.extensions["timeout"]
        assert timeout["connect"] == 120.0

    @respx.mock
    def test_download_file_sync_progress(self, mock_env_clear, mock_blob_head_response, tmp_path):
        """Test sync file download writes bytes and emits progress."""
        payload = b"download-sync-payload"
        route = respx.get(mock_blob_head_response["downloadUrl"]).mock(
            return_value=httpx.Response(
                200,
                content=payload,
                headers={"Content-Length": str(len(payload))},
            )
        )
        destination = tmp_path / "sync-download.bin"
        progress_updates: list[tuple[int, int | None]] = []

        result = download_file(
            mock_blob_head_response["downloadUrl"],
            destination,
            token="test_token",
            progress=lambda loaded, total: progress_updates.append((loaded, total)),
        )

        assert route.called
        assert result == str(destination)
        assert destination.read_bytes() == payload
        assert progress_updates[-1] == (len(payload), len(payload))

    @respx.mock
    def test_download_file_sync_progress_per_chunk(
        self, mock_env_clear, mock_blob_head_response, tmp_path
    ):
        """Test sync file download emits progress for each streamed chunk."""
        chunks = [b"chunk-1", b"chunk-2"]
        payload = b"".join(chunks)

        class ChunkedSyncStream(httpx.SyncByteStream):
            def __iter__(self):
                yield from chunks

        route = respx.get(mock_blob_head_response["downloadUrl"]).mock(
            return_value=httpx.Response(
                200,
                stream=ChunkedSyncStream(),
                headers={"Content-Length": str(len(payload))},
            )
        )
        destination = tmp_path / "sync-download-chunked.bin"
        progress_updates: list[tuple[int, int | None]] = []

        result = download_file(
            mock_blob_head_response["downloadUrl"],
            destination,
            token="test_token",
            progress=lambda loaded, total: progress_updates.append((loaded, total)),
        )

        assert route.called
        assert result == str(destination)
        assert destination.read_bytes() == payload
        assert len(progress_updates) >= 2
        assert progress_updates[-1] == (len(payload), len(payload))
        assert any(update[0] < len(payload) for update in progress_updates)

    @respx.mock
    @pytest.mark.asyncio
    async def test_download_file_async_progress(
        self, mock_env_clear, mock_blob_head_response, tmp_path
    ):
        """Test async file download supports awaitable progress callbacks."""
        payload = b"download-async-payload"
        head_route = respx.get(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_head_response)
        )
        download_route = respx.get(mock_blob_head_response["downloadUrl"]).mock(
            return_value=httpx.Response(
                200,
                content=payload,
                headers={"Content-Length": str(len(payload))},
            )
        )
        destination = tmp_path / "async-download.bin"
        progress_updates: list[tuple[int, int | None]] = []

        async def progress_callback(loaded: int, total: int | None) -> None:
            progress_updates.append((loaded, total))

        result = await download_file_async(
            "test.txt",
            destination,
            token="test_token",
            progress=progress_callback,
        )

        assert head_route.called
        assert download_route.called
        assert result == str(destination)
        assert destination.read_bytes() == payload
        assert progress_updates[-1] == (len(payload), len(payload))

    @respx.mock
    @pytest.mark.asyncio
    async def test_download_file_async_progress_per_chunk(
        self, mock_env_clear, mock_blob_head_response, tmp_path
    ):
        """Test async file download emits progress for each streamed chunk."""
        chunks = [b"chunk-a", b"chunk-b"]
        payload = b"".join(chunks)

        class ChunkedAsyncStream(httpx.AsyncByteStream):
            async def __aiter__(self):
                for chunk in chunks:
                    yield chunk

        head_route = respx.get(BLOB_API_BASE).mock(
            return_value=httpx.Response(200, json=mock_blob_head_response)
        )
        download_route = respx.get(mock_blob_head_response["downloadUrl"]).mock(
            return_value=httpx.Response(
                200,
                stream=ChunkedAsyncStream(),
                headers={"Content-Length": str(len(payload))},
            )
        )
        destination = tmp_path / "async-download-chunked.bin"
        progress_updates: list[tuple[int, int | None]] = []

        async def progress_callback(loaded: int, total: int | None) -> None:
            progress_updates.append((loaded, total))

        result = await download_file_async(
            "test.txt",
            destination,
            token="test_token",
            progress=progress_callback,
        )

        assert head_route.called
        assert download_route.called
        assert result == str(destination)
        assert destination.read_bytes() == payload
        assert len(progress_updates) >= 2
        assert progress_updates[-1] == (len(payload), len(payload))
        assert any(update[0] < len(payload) for update in progress_updates)


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

    @respx.mock
    def test_iter_objects_sync_batch_size_limit_pagination(self, mock_env_clear):
        """Test sync iteration uses paginated list requests with limit-aware batching."""
        first_page = {
            "blobs": [
                {
                    "url": "https://blob.vercel-storage.com/test-abc123/page1.txt",
                    "downloadUrl": "https://blob.vercel-storage.com/test-abc123/page1.txt?download=1",
                    "pathname": "page1.txt",
                    "contentType": "text/plain",
                    "contentDisposition": 'inline; filename="page1.txt"',
                    "size": 50,
                    "uploadedAt": "2024-01-15T10:30:00.000Z",
                }
            ],
            "cursor": "cursor-1",
            "hasMore": True,
            "folders": [],
        }
        second_page = {
            "blobs": [
                {
                    "url": "https://blob.vercel-storage.com/test-abc123/page2.txt",
                    "downloadUrl": "https://blob.vercel-storage.com/test-abc123/page2.txt?download=1",
                    "pathname": "page2.txt",
                    "contentType": "text/plain",
                    "contentDisposition": 'inline; filename="page2.txt"',
                    "size": 60,
                    "uploadedAt": "2024-01-15T10:31:00.000Z",
                }
            ],
            "cursor": "cursor-2",
            "hasMore": True,
            "folders": [],
        }
        route = respx.get(BLOB_API_BASE).mock(
            side_effect=[
                httpx.Response(200, json=first_page),
                httpx.Response(200, json=second_page),
            ]
        )

        items = list(iter_objects(batch_size=1, limit=2, token="test_token"))

        assert [item.pathname for item in items] == ["page1.txt", "page2.txt"]
        assert route.call_count == 2
        assert route.calls[0].request.url.params.get("limit") == "1"
        assert route.calls[0].request.url.params.get("cursor") is None
        assert route.calls[1].request.url.params.get("limit") == "1"
        assert route.calls[1].request.url.params.get("cursor") == "cursor-1"

    @respx.mock
    @pytest.mark.asyncio
    async def test_iter_objects_async_batch_size_limit_pagination(self, mock_env_clear):
        """Test async iteration uses paginated list requests with limit-aware batching."""
        first_page = {
            "blobs": [
                {
                    "url": "https://blob.vercel-storage.com/test-abc123/page1.txt",
                    "downloadUrl": "https://blob.vercel-storage.com/test-abc123/page1.txt?download=1",
                    "pathname": "page1.txt",
                    "contentType": "text/plain",
                    "contentDisposition": 'inline; filename="page1.txt"',
                    "size": 50,
                    "uploadedAt": "2024-01-15T10:30:00.000Z",
                }
            ],
            "cursor": "cursor-1",
            "hasMore": True,
            "folders": [],
        }
        second_page = {
            "blobs": [
                {
                    "url": "https://blob.vercel-storage.com/test-abc123/page2.txt",
                    "downloadUrl": "https://blob.vercel-storage.com/test-abc123/page2.txt?download=1",
                    "pathname": "page2.txt",
                    "contentType": "text/plain",
                    "contentDisposition": 'inline; filename="page2.txt"',
                    "size": 60,
                    "uploadedAt": "2024-01-15T10:31:00.000Z",
                }
            ],
            "cursor": "cursor-2",
            "hasMore": True,
            "folders": [],
        }
        route = respx.get(BLOB_API_BASE).mock(
            side_effect=[
                httpx.Response(200, json=first_page),
                httpx.Response(200, json=second_page),
            ]
        )

        items = []
        async for item in iter_objects_async(batch_size=1, limit=2, token="test_token"):
            items.append(item)

        assert [item.pathname for item in items] == ["page1.txt", "page2.txt"]
        assert route.call_count == 2
        assert route.calls[0].request.url.params.get("limit") == "1"
        assert route.calls[0].request.url.params.get("cursor") is None
        assert route.calls[1].request.url.params.get("limit") == "1"
        assert route.calls[1].request.url.params.get("cursor") == "cursor-1"


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
