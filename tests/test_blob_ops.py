"""Unit tests for blob operations added/changed by PR #50 (private blob support)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import get_args
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from vercel._internal.blob import validate_access
from vercel._internal.blob.core import parse_last_modified
from vercel._internal.iter_coroutine import iter_coroutine
from vercel.blob.errors import BlobError
from vercel.blob.ops import (
    download_file,
    download_file_async,
)

# Token format: vercel_blob_rw_{storeId}_...
# extract_store_id_from_token splits on "_" and returns index 3
TOKEN = "vercel_blob_rw_storeid123_token123"
STORE_ID = "storeid123"


# ---------------------------------------------------------------------------
# parse_last_modified — pure logic
# ---------------------------------------------------------------------------
class TestParseLastModified:
    def test_rfc7231_date(self):
        dt = parse_last_modified("Tue, 15 Nov 1994 08:12:31 GMT")
        assert dt == datetime(1994, 11, 15, 8, 12, 31, tzinfo=timezone.utc)

    def test_iso8601_date(self):
        dt = parse_last_modified("2024-01-15T10:30:00+00:00")
        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 15
        assert dt.hour == 10
        assert dt.minute == 30

    def test_none_returns_approx_now(self):
        before = datetime.now(tz=timezone.utc)
        dt = parse_last_modified(None)
        after = datetime.now(tz=timezone.utc)
        assert before <= dt <= after

    def test_invalid_string_returns_approx_now(self):
        before = datetime.now(tz=timezone.utc)
        dt = parse_last_modified("not-a-date")
        after = datetime.now(tz=timezone.utc)
        assert before <= dt <= after


# ---------------------------------------------------------------------------
# validate_access — pure logic
# ---------------------------------------------------------------------------
class TestValidateAccess:
    def test_public(self):
        assert validate_access("public") == "public"

    def test_private(self):
        assert validate_access("private") == "private"

    def test_invalid_raises_blob_error(self):
        with pytest.raises(BlobError):
            validate_access("invalid")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# download_file (sync) — wrapper delegation
# ---------------------------------------------------------------------------
class TestDownloadFile:
    def test_pathname_delegates_to_core_client(self, tmp_path):
        dest = tmp_path / "downloaded.txt"
        mock_core_client = MagicMock()
        mock_core_client.download_file = AsyncMock(return_value=str(dest))

        def _run(operation):
            return iter_coroutine(operation(mock_core_client))

        with patch("vercel.blob.ops._run_sync_blob_operation", side_effect=_run):
            result = download_file("my/file.txt", str(dest), token=TOKEN, access="public")

        assert result == str(dest)
        mock_core_client.download_file.assert_awaited_once_with(
            "my/file.txt",
            str(dest),
            access="public",
            token=TOKEN,
            timeout=None,
            overwrite=True,
            create_parents=True,
            progress=None,
        )

    def test_private_access_passes_access_to_core_client(self, tmp_path):
        dest = tmp_path / "private.txt"
        mock_core_client = MagicMock()
        mock_core_client.download_file = AsyncMock(return_value=str(dest))

        def _run(operation):
            return iter_coroutine(operation(mock_core_client))

        with patch("vercel.blob.ops._run_sync_blob_operation", side_effect=_run):
            download_file("my/secret.txt", str(dest), token=TOKEN, access="private")

        kwargs = mock_core_client.download_file.await_args.kwargs
        assert kwargs["access"] == "private"
        assert kwargs["token"] == TOKEN


# ---------------------------------------------------------------------------
# download_file_async — wrapper delegation
# ---------------------------------------------------------------------------
class TestDownloadFileAsync:
    async def test_pathname_delegates_to_core_client(self, tmp_path):
        dest = tmp_path / "downloaded_async.txt"
        mock_core_client = MagicMock()
        mock_core_client.download_file = AsyncMock(return_value=str(dest))
        mock_core_client.__aenter__ = AsyncMock(return_value=mock_core_client)
        mock_core_client.__aexit__ = AsyncMock(return_value=False)

        with patch("vercel.blob.ops.AsyncBlobOpsClient", return_value=mock_core_client):
            result = await download_file_async(
                "my/file.txt", str(dest), token=TOKEN, access="public"
            )

        assert result == str(dest)
        mock_core_client.download_file.assert_awaited_once_with(
            "my/file.txt",
            str(dest),
            access="public",
            token=TOKEN,
            timeout=None,
            overwrite=True,
            create_parents=True,
            progress=None,
        )

    async def test_private_access_passes_access_to_core_client(self, tmp_path):
        dest = tmp_path / "private_async.txt"
        mock_core_client = MagicMock()
        mock_core_client.download_file = AsyncMock(return_value=str(dest))
        mock_core_client.__aenter__ = AsyncMock(return_value=mock_core_client)
        mock_core_client.__aexit__ = AsyncMock(return_value=False)

        with patch("vercel.blob.ops.AsyncBlobOpsClient", return_value=mock_core_client):
            await download_file_async("my/secret.txt", str(dest), token=TOKEN, access="private")

        kwargs = mock_core_client.download_file.await_args.kwargs
        assert kwargs["access"] == "private"
        assert kwargs["token"] == TOKEN


# ---------------------------------------------------------------------------
# Access type export
# ---------------------------------------------------------------------------
class TestAccessTypeExport:
    def test_import_from_blob(self):
        from vercel.blob import Access as BlobAccess

        assert get_args(BlobAccess) == ("public", "private")

    def test_import_from_blob_aio(self):
        from vercel.blob.aio import Access as AioAccess

        assert get_args(AioAccess) == ("public", "private")
