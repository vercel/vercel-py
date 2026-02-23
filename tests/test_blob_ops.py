"""Unit tests for blob operations added/changed by PR #50 (private blob support)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import get_args
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from vercel._iter_coroutine import iter_coroutine
from vercel.blob.errors import BlobError
from vercel.blob.ops import (
    _build_get_result,
    _parse_last_modified,
    _resolve_blob_url,
    download_file,
    download_file_async,
)
from vercel.blob.utils import validate_access

# Token format: vercel_blob_rw_{storeId}_...
# extract_store_id_from_token splits on "_" and returns index 3
TOKEN = "vercel_blob_rw_storeid123_token123"
STORE_ID = "storeid123"


# ---------------------------------------------------------------------------
# _resolve_blob_url — pure logic, no mocking
# ---------------------------------------------------------------------------
class TestResolveBlobUrl:
    def test_url_input_returns_same_url_and_pathname(self):
        url = "https://example.com/foo/bar.txt"
        result_url, pathname = _resolve_blob_url(url, TOKEN, "public")
        assert result_url == url
        assert pathname == "foo/bar.txt"

    def test_pathname_public_access(self):
        blob_url, pathname = _resolve_blob_url("my/file.txt", TOKEN, "public")
        expected = f"https://{STORE_ID}.public.blob.vercel-storage.com/my/file.txt"
        assert blob_url == expected
        assert pathname == "my/file.txt"

    def test_pathname_private_access(self):
        blob_url, pathname = _resolve_blob_url("my/file.txt", TOKEN, "private")
        expected = f"https://{STORE_ID}.private.blob.vercel-storage.com/my/file.txt"
        assert blob_url == expected
        assert pathname == "my/file.txt"

    def test_bad_token_raises_blob_error(self):
        with pytest.raises(BlobError):
            _resolve_blob_url("my/file.txt", "short_token", "public")

    def test_leading_slash_stripped(self):
        blob_url, pathname = _resolve_blob_url("/leading/slash.txt", TOKEN, "public")
        assert pathname == "leading/slash.txt"
        # No double-slash in the path portion (ignore the scheme "https://")
        path_part = blob_url.split("://", 1)[1]
        assert "//" not in path_part


# ---------------------------------------------------------------------------
# _parse_last_modified — pure logic
# ---------------------------------------------------------------------------
class TestParseLastModified:
    def test_rfc7231_date(self):
        dt = _parse_last_modified("Tue, 15 Nov 1994 08:12:31 GMT")
        assert dt == datetime(1994, 11, 15, 8, 12, 31, tzinfo=timezone.utc)

    def test_iso8601_date(self):
        dt = _parse_last_modified("2024-01-15T10:30:00+00:00")
        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 15
        assert dt.hour == 10
        assert dt.minute == 30

    def test_none_returns_approx_now(self):
        before = datetime.now(tz=timezone.utc)
        dt = _parse_last_modified(None)
        after = datetime.now(tz=timezone.utc)
        assert before <= dt <= after

    def test_invalid_string_returns_approx_now(self):
        before = datetime.now(tz=timezone.utc)
        dt = _parse_last_modified("not-a-date")
        after = datetime.now(tz=timezone.utc)
        assert before <= dt <= after


# ---------------------------------------------------------------------------
# _build_get_result — mock httpx.Response
# ---------------------------------------------------------------------------
class TestBuildGetResult:
    @staticmethod
    def _make_response(status_code: int, headers: dict, content: bytes = b""):
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = status_code
        resp.headers = httpx.Headers(headers)
        resp.content = content
        return resp

    def test_200_response(self):
        resp = self._make_response(
            200,
            {
                "content-type": "text/plain",
                "content-length": "13",
                "content-disposition": "inline",
                "cache-control": "max-age=300",
                "last-modified": "2024-01-15T10:30:00+00:00",
                "etag": '"abc"',
            },
            content=b"Hello, world!",
        )
        result = _build_get_result(
            resp, "https://s.public.blob.vercel-storage.com/f.txt", "f.txt"
        )
        assert result.status_code == resp.status_code
        assert result.content == b"Hello, world!"
        assert result.size == 13
        assert result.content_type == "text/plain"

    def test_304_response(self):
        resp = self._make_response(
            304,
            {
                "content-disposition": "inline",
                "cache-control": "max-age=300",
                "etag": '"abc"',
            },
        )
        result = _build_get_result(
            resp, "https://s.public.blob.vercel-storage.com/f.txt", "f.txt"
        )
        assert result.status_code == 304
        assert result.content == b""
        assert result.size is None
        assert result.content_type is None


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
        mock_core_client._download_file = AsyncMock(return_value=str(dest))

        def _run(operation):
            return iter_coroutine(operation(mock_core_client))

        with patch("vercel.blob.ops._run_sync_blob_operation", side_effect=_run):
            result = download_file(
                "my/file.txt", str(dest), token=TOKEN, access="public"
            )

        assert result == str(dest)
        mock_core_client._download_file.assert_awaited_once_with(
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
        mock_core_client._download_file = AsyncMock(return_value=str(dest))

        def _run(operation):
            return iter_coroutine(operation(mock_core_client))

        with patch("vercel.blob.ops._run_sync_blob_operation", side_effect=_run):
            download_file(
                "my/secret.txt", str(dest), token=TOKEN, access="private"
            )

        kwargs = mock_core_client._download_file.await_args.kwargs
        assert kwargs["access"] == "private"
        assert kwargs["token"] == TOKEN


# ---------------------------------------------------------------------------
# download_file_async — wrapper delegation
# ---------------------------------------------------------------------------
class TestDownloadFileAsync:
    async def test_pathname_delegates_to_core_client(self, tmp_path):
        dest = tmp_path / "downloaded_async.txt"
        mock_core_client = MagicMock()
        mock_core_client._download_file = AsyncMock(return_value=str(dest))
        mock_core_client.__aenter__ = AsyncMock(return_value=mock_core_client)
        mock_core_client.__aexit__ = AsyncMock(return_value=False)

        with patch("vercel.blob.ops._AsyncBlobOpsClient", return_value=mock_core_client):
            result = await download_file_async(
                "my/file.txt", str(dest), token=TOKEN, access="public"
            )

        assert result == str(dest)
        mock_core_client._download_file.assert_awaited_once_with(
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
        mock_core_client._download_file = AsyncMock(return_value=str(dest))
        mock_core_client.__aenter__ = AsyncMock(return_value=mock_core_client)
        mock_core_client.__aexit__ = AsyncMock(return_value=False)

        with patch("vercel.blob.ops._AsyncBlobOpsClient", return_value=mock_core_client):
            await download_file_async(
                "my/secret.txt", str(dest), token=TOKEN, access="private"
            )

        kwargs = mock_core_client._download_file.await_args.kwargs
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
