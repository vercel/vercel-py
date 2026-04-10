from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from vercel.blob.client import AsyncBlobClient, BlobClient
from vercel.blob.errors import BlobError
from vercel.blob.types import HeadBlobResult, ListBlobResult


def _head_result() -> HeadBlobResult:
    return HeadBlobResult(
        size=1,
        uploaded_at=datetime.now(timezone.utc),
        pathname="file.txt",
        content_type="text/plain",
        content_disposition="inline",
        url="https://blob.vercel-storage.com/file.txt",
        download_url="https://blob.vercel-storage.com/file.txt?download=1",
        cache_control="public, max-age=3600",
    )


def _list_result() -> ListBlobResult:
    return ListBlobResult(blobs=[], cursor=None, has_more=False)


class TestBlobClientLifecycle:
    def test_sync_client_reuses_owned_ops_client(self) -> None:
        mock_ops_client = MagicMock()
        mock_ops_client.head_blob = AsyncMock(return_value=_head_result())
        mock_ops_client.list_objects = MagicMock(return_value=_list_result())

        with patch("vercel.blob.client.SyncBlobOpsClient", return_value=mock_ops_client) as ctor:
            client = BlobClient(token="test_token")
            client.head("file.txt")
            client.list_objects()

        assert ctor.call_count == 1
        mock_ops_client.head_blob.assert_awaited_once()
        mock_ops_client.list_objects.assert_called_once()

    def test_sync_close_is_idempotent_and_blocks_use_after_close(self) -> None:
        mock_ops_client = MagicMock()
        mock_ops_client.head_blob = AsyncMock(return_value=_head_result())

        with patch("vercel.blob.client.SyncBlobOpsClient", return_value=mock_ops_client):
            client = BlobClient(token="test_token")
            client.close()
            client.close()

            with pytest.raises(BlobError, match="Client is closed"):
                client.head("file.txt")

        mock_ops_client.close.assert_called_once()
        mock_ops_client.head_blob.assert_not_called()

    def test_sync_client_multipart_uploader_uses_ownedrequest_api(self) -> None:
        actions: list[str] = []

        async def request_api(**kwargs):
            action = kwargs["headers"]["x-mpu-action"]
            actions.append(action)
            if action == "create":
                return {"uploadId": "upload-id", "key": "blob-key"}
            if action == "upload":
                return {"etag": "etag-1"}
            return {
                "url": "https://blob.vercel-storage.com/test-abc123/folder/client-mpu.bin",
                "downloadUrl": (
                    "https://blob.vercel-storage.com/test-abc123/folder/client-mpu.bin?download=1"
                ),
                "pathname": "folder/client-mpu.bin",
                "contentType": "application/octet-stream",
                "contentDisposition": 'inline; filename="client-mpu.bin"',
            }

        mock_request_client = MagicMock()
        mock_request_client.request_api = AsyncMock(side_effect=request_api)
        mock_ops_client = MagicMock()
        mock_ops_client._request_client = mock_request_client

        with patch("vercel.blob.client.SyncBlobOpsClient", return_value=mock_ops_client):
            client = BlobClient(token="test_token")
            uploader = client.create_multipart_uploader("folder/client-mpu.bin")
            part = uploader.upload_part(1, b"chunk")
            result = uploader.complete([part])

        assert actions == ["create", "upload", "complete"]
        assert mock_request_client.request_api.await_count == 3
        assert result.pathname == "folder/client-mpu.bin"

    @pytest.mark.asyncio
    async def test_async_client_reuses_owned_ops_client(self) -> None:
        mock_ops_client = MagicMock()
        mock_ops_client.head_blob = AsyncMock(return_value=_head_result())
        mock_ops_client.list_objects = AsyncMock(return_value=_list_result())

        with patch("vercel.blob.client.AsyncBlobOpsClient", return_value=mock_ops_client) as ctor:
            client = AsyncBlobClient(token="test_token")
            await client.head("file.txt")
            await client.list_objects()

        assert ctor.call_count == 1
        mock_ops_client.head_blob.assert_awaited_once()
        mock_ops_client.list_objects.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_async_close_is_idempotent_and_blocks_use_after_close(self) -> None:
        mock_ops_client = MagicMock()
        mock_ops_client.aclose = AsyncMock()
        mock_ops_client.head_blob = AsyncMock(return_value=_head_result())

        with patch("vercel.blob.client.AsyncBlobOpsClient", return_value=mock_ops_client):
            client = AsyncBlobClient(token="test_token")
            await client.aclose()
            await client.aclose()

            with pytest.raises(BlobError, match="Client is closed"):
                await client.head("file.txt")

        mock_ops_client.aclose.assert_awaited_once()
        mock_ops_client.head_blob.assert_not_called()

    @pytest.mark.asyncio
    async def test_async_client_multipart_uploader_uses_ownedrequest_api(self) -> None:
        actions: list[str] = []

        async def request_api(**kwargs):
            action = kwargs["headers"]["x-mpu-action"]
            actions.append(action)
            if action == "create":
                return {"uploadId": "upload-id", "key": "blob-key"}
            if action == "upload":
                return {"etag": "etag-1"}
            return {
                "url": "https://blob.vercel-storage.com/test-abc123/folder/client-mpu-async.bin",
                "downloadUrl": (
                    "https://blob.vercel-storage.com/test-abc123/folder/"
                    "client-mpu-async.bin?download=1"
                ),
                "pathname": "folder/client-mpu-async.bin",
                "contentType": "application/octet-stream",
                "contentDisposition": 'inline; filename="client-mpu-async.bin"',
            }

        mock_request_client = MagicMock()
        mock_request_client.request_api = AsyncMock(side_effect=request_api)
        mock_ops_client = MagicMock()
        mock_ops_client._request_client = mock_request_client

        with patch("vercel.blob.client.AsyncBlobOpsClient", return_value=mock_ops_client):
            client = AsyncBlobClient(token="test_token")
            uploader = await client.create_multipart_uploader("folder/client-mpu-async.bin")
            part = await uploader.upload_part(1, b"chunk")
            result = await uploader.complete([part])

        assert actions == ["create", "upload", "complete"]
        assert mock_request_client.request_api.await_count == 3
        assert result.pathname == "folder/client-mpu-async.bin"
