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
            client = BlobClient()
            client.head("file.txt")
            client.list_objects()

        assert ctor.call_count == 1
        ctor.assert_called_once_with(token=None)
        mock_ops_client.head_blob.assert_awaited_once_with("file.txt", token=None)
        mock_ops_client.list_objects.assert_called_once_with(
            limit=None, prefix=None, cursor=None, mode=None, token=None
        )

    def test_sync_client_passes_per_method_token(self) -> None:
        mock_ops_client = MagicMock()
        mock_ops_client.head_blob = AsyncMock(return_value=_head_result())
        mock_ops_client.list_objects = MagicMock(return_value=_list_result())

        with patch("vercel.blob.client.SyncBlobOpsClient", return_value=mock_ops_client):
            client = BlobClient()
            client.head("file.txt", token="per_call_token")
            client.list_objects(token="per_call_token")

        mock_ops_client.head_blob.assert_awaited_once_with("file.txt", token="per_call_token")
        mock_ops_client.list_objects.assert_called_once_with(
            limit=None, prefix=None, cursor=None, mode=None, token="per_call_token"
        )

    def test_sync_client_accepts_client_token(self) -> None:
        mock_ops_client = MagicMock()

        with patch("vercel.blob.client.SyncBlobOpsClient", return_value=mock_ops_client) as ctor:
            BlobClient(token="client_token")

        ctor.assert_called_once_with(token="client_token")

    def test_sync_client_accepts_positional_token(self) -> None:
        mock_ops_client = MagicMock()

        with patch("vercel.blob.client.SyncBlobOpsClient", return_value=mock_ops_client) as ctor:
            BlobClient("client_token")

        ctor.assert_called_once_with(token="client_token")

    def test_sync_close_is_idempotent_and_blocks_use_after_close(self) -> None:
        mock_ops_client = MagicMock()
        mock_ops_client.head_blob = AsyncMock(return_value=_head_result())

        with patch("vercel.blob.client.SyncBlobOpsClient", return_value=mock_ops_client):
            client = BlobClient()
            client.close()
            client.close()

            with pytest.raises(BlobError, match="Client is closed"):
                client.head("file.txt")

        mock_ops_client.close.assert_called_once()
        mock_ops_client.head_blob.assert_not_called()

    def test_sync_client_multipart_uploader_uses_owned_request_api(self) -> None:
        actions: list[str] = []
        tokens: list[str | None] = []

        async def request_api(**kwargs):
            tokens.append(kwargs["token"])
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
        mock_request_client.resolve_token = AsyncMock(
            side_effect=["create_token", "part_token", "complete_token"]
        )
        mock_ops_client = MagicMock()
        mock_ops_client._request_client = mock_request_client

        with patch("vercel.blob.client.SyncBlobOpsClient", return_value=mock_ops_client) as ctor:
            client = BlobClient(token="client_token")
            uploader = client.create_multipart_uploader("folder/client-mpu.bin")
            part = uploader.upload_part(1, b"chunk")
            result = uploader.complete([part])

        ctor.assert_called_once_with(token="client_token")
        assert actions == ["create", "upload", "complete"]
        assert tokens == ["create_token", "part_token", "complete_token"]
        assert mock_request_client.resolve_token.await_count == 3
        assert mock_request_client.request_api.await_count == 3
        assert result.pathname == "folder/client-mpu.bin"

    @pytest.mark.asyncio
    async def test_async_client_reuses_owned_ops_client(self) -> None:
        mock_ops_client = MagicMock()
        mock_ops_client.head_blob = AsyncMock(return_value=_head_result())
        mock_ops_client.list_objects = AsyncMock(return_value=_list_result())

        with patch("vercel.blob.client.AsyncBlobOpsClient", return_value=mock_ops_client) as ctor:
            client = AsyncBlobClient()
            await client.head("file.txt")
            await client.list_objects()

        assert ctor.call_count == 1
        ctor.assert_called_once_with(token=None)
        mock_ops_client.head_blob.assert_awaited_once_with("file.txt", token=None)
        mock_ops_client.list_objects.assert_awaited_once_with(
            limit=None, prefix=None, cursor=None, mode=None, token=None
        )

    @pytest.mark.asyncio
    async def test_async_client_passes_per_method_token(self) -> None:
        mock_ops_client = MagicMock()
        mock_ops_client.head_blob = AsyncMock(return_value=_head_result())
        mock_ops_client.list_objects = AsyncMock(return_value=_list_result())

        with patch("vercel.blob.client.AsyncBlobOpsClient", return_value=mock_ops_client):
            client = AsyncBlobClient()
            await client.head("file.txt", token="per_call_token")
            await client.list_objects(token="per_call_token")

        mock_ops_client.head_blob.assert_awaited_once_with("file.txt", token="per_call_token")
        mock_ops_client.list_objects.assert_awaited_once_with(
            limit=None, prefix=None, cursor=None, mode=None, token="per_call_token"
        )

    @pytest.mark.asyncio
    async def test_async_client_accepts_client_token(self) -> None:
        mock_ops_client = MagicMock()

        with patch("vercel.blob.client.AsyncBlobOpsClient", return_value=mock_ops_client) as ctor:
            AsyncBlobClient(token="client_token")

        ctor.assert_called_once_with(token="client_token")

    @pytest.mark.asyncio
    async def test_async_client_accepts_positional_token(self) -> None:
        mock_ops_client = MagicMock()

        with patch("vercel.blob.client.AsyncBlobOpsClient", return_value=mock_ops_client) as ctor:
            AsyncBlobClient("client_token")

        ctor.assert_called_once_with(token="client_token")

    @pytest.mark.asyncio
    async def test_async_close_is_idempotent_and_blocks_use_after_close(self) -> None:
        mock_ops_client = MagicMock()
        mock_ops_client.aclose = AsyncMock()
        mock_ops_client.head_blob = AsyncMock(return_value=_head_result())

        with patch("vercel.blob.client.AsyncBlobOpsClient", return_value=mock_ops_client):
            client = AsyncBlobClient()
            await client.aclose()
            await client.aclose()

            with pytest.raises(BlobError, match="Client is closed"):
                await client.head("file.txt")

        mock_ops_client.aclose.assert_awaited_once()
        mock_ops_client.head_blob.assert_not_called()

    @pytest.mark.asyncio
    async def test_async_client_multipart_uploader_uses_owned_request_api(self) -> None:
        actions: list[str] = []
        tokens: list[str | None] = []

        async def request_api(**kwargs):
            tokens.append(kwargs["token"])
            action = kwargs["headers"]["x-mpu-action"]
            actions.append(action)
            if action == "create":
                return {"uploadId": "upload-id", "key": "blob-key"}
            if action == "upload":
                return {"etag": "etag-1"}
            return {
                "url": ("https://blob.vercel-storage.com/test-abc123/folder/client-mpu-async.bin"),
                "downloadUrl": (
                    "https://blob.vercel-storage.com/"
                    "test-abc123/folder/client-mpu-async.bin?download=1"
                ),
                "pathname": "folder/client-mpu-async.bin",
                "contentType": "application/octet-stream",
                "contentDisposition": 'inline; filename="client-mpu-async.bin"',
            }

        mock_request_client = MagicMock()
        mock_request_client.request_api = AsyncMock(side_effect=request_api)
        mock_request_client.resolve_token = AsyncMock(
            side_effect=["create_token", "part_token", "complete_token"]
        )
        mock_ops_client = MagicMock()
        mock_ops_client._request_client = mock_request_client

        with patch("vercel.blob.client.AsyncBlobOpsClient", return_value=mock_ops_client) as ctor:
            client = AsyncBlobClient(token="client_token")
            uploader = await client.create_multipart_uploader("folder/client-mpu-async.bin")
            part = await uploader.upload_part(1, b"chunk")
            result = await uploader.complete([part])

        ctor.assert_called_once_with(token="client_token")
        assert actions == ["create", "upload", "complete"]
        assert tokens == ["create_token", "part_token", "complete_token"]
        assert mock_request_client.resolve_token.await_count == 3
        assert mock_request_client.request_api.await_count == 3
        assert result.pathname == "folder/client-mpu-async.bin"
