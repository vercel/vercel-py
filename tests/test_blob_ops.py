"""Unit tests for blob operations added/changed by PR #50 (private blob support)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import get_args
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

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
# download_file (sync) — mock httpx.Client
# ---------------------------------------------------------------------------
class TestDownloadFile:
    @staticmethod
    def _mock_sync_download(chunk_data: bytes):
        """Return a mock httpx.Client whose stream() yields *chunk_data*."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = httpx.Headers(
            {"Content-Length": str(len(chunk_data))}
        )
        mock_resp.iter_bytes.return_value = iter([chunk_data])
        mock_resp.raise_for_status = MagicMock()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)

        mock_client = MagicMock()
        mock_client.stream.return_value = mock_resp
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        return mock_client

    def test_pathname_constructs_url_and_writes_file(self, tmp_path):
        dest = tmp_path / "downloaded.txt"
        chunk_data = b"file content here"
        mock_client = self._mock_sync_download(chunk_data)

        with patch("vercel.blob.ops.httpx.Client", return_value=mock_client):
            result = download_file(
                "my/file.txt", str(dest), token=TOKEN, access="public"
            )

        assert result == str(dest)
        assert dest.read_bytes() == chunk_data
        # URL was constructed from pathname — no head() call needed
        mock_client.stream.assert_called_once()
        url_arg = mock_client.stream.call_args[0][1]
        assert STORE_ID in url_arg

    def test_private_access_sends_auth_header(self, tmp_path):
        dest = tmp_path / "private.txt"
        mock_client = self._mock_sync_download(b"secret")

        with patch("vercel.blob.ops.httpx.Client", return_value=mock_client):
            download_file(
                "my/secret.txt", str(dest), token=TOKEN, access="private"
            )

        headers = mock_client.stream.call_args[1]["headers"]
        assert headers["authorization"] == f"Bearer {TOKEN}"


# ---------------------------------------------------------------------------
# download_file_async — mock httpx.AsyncClient
# ---------------------------------------------------------------------------
class TestDownloadFileAsync:
    @staticmethod
    def _mock_async_download(chunk_data: bytes):
        """Return a mock httpx.AsyncClient whose stream() yields *chunk_data*."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = httpx.Headers(
            {"Content-Length": str(len(chunk_data))}
        )
        mock_resp.raise_for_status = MagicMock()

        async def aiter_bytes():
            yield chunk_data

        mock_resp.aiter_bytes = aiter_bytes
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_client = MagicMock()
        mock_client.stream.return_value = mock_resp
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        return mock_client

    async def test_pathname_constructs_url_and_writes_file(self, tmp_path):
        dest = tmp_path / "downloaded_async.txt"
        chunk_data = b"async file content"
        mock_client = self._mock_async_download(chunk_data)

        with patch(
            "vercel.blob.ops.httpx.AsyncClient", return_value=mock_client
        ):
            result = await download_file_async(
                "my/file.txt", str(dest), token=TOKEN, access="public"
            )

        assert result == str(dest)
        assert dest.read_bytes() == chunk_data

    async def test_private_access_sends_auth_header(self, tmp_path):
        dest = tmp_path / "private_async.txt"
        mock_client = self._mock_async_download(b"async secret")

        with patch(
            "vercel.blob.ops.httpx.AsyncClient", return_value=mock_client
        ):
            await download_file_async(
                "my/secret.txt", str(dest), token=TOKEN, access="private"
            )

        headers = mock_client.stream.call_args[1]["headers"]
        assert headers["authorization"] == f"Bearer {TOKEN}"


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
