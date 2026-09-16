"""HTTP request client and response parsing for Blob."""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Coroutine
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any, cast
from urllib.parse import urlsplit

import httpx

from vercel._internal.core.http import BaseTransport, JSONBody, RawBody, ReadResponsePolicy
from vercel.blob.errors import (
    BlobAccessError,
    BlobContentTypeNotAllowedError,
    BlobCredentialsError,
    BlobError,
    BlobFileTooLargeError,
    BlobNotFoundError,
    BlobPathnameMismatchError,
    BlobPreconditionFailedError,
    BlobServiceNotAvailable,
    BlobServiceRateLimited,
    BlobStoreNotFoundError,
    BlobStoreSuspendedError,
    BlobStreamError,
    BlobUnknownError,
)

from .credentials import _normalize_credentials
from .models import Access, BlobCredentials, BlobStatResult

_CONTENT_RANGE = re.compile(r"bytes (\d+)-(\d+)/(\d+)")


def _has_control_character(value: str) -> bool:
    return any(ord(character) < 32 or ord(character) == 127 for character in value)


def _is_absolute_http_url(value: object) -> bool:
    if not isinstance(value, str):
        return False
    parsed = urlsplit(value)
    return parsed.scheme in ("http", "https") and bool(parsed.netloc)


def _parse_stat(payload: Any) -> BlobStatResult:
    if not isinstance(payload, dict):
        raise BlobStreamError("Blob stat response must be an object")
    try:
        pathname = payload["pathname"]
        url = payload["url"]
        download_url = payload["downloadUrl"]
        size = payload["size"]
        etag = payload["etag"]
        uploaded_at = payload["uploadedAt"]
    except KeyError as exc:
        raise BlobStreamError(f"Blob stat response is missing {exc.args[0]}") from exc
    if not isinstance(pathname, str) or not pathname or _has_control_character(pathname):
        raise BlobStreamError("Blob stat pathname is invalid")
    if not _is_absolute_http_url(url) or not _is_absolute_http_url(download_url):
        raise BlobStreamError("Blob stat URLs are invalid")
    if isinstance(size, bool) or not isinstance(size, int) or size < 0:
        raise BlobStreamError("Blob stat size is invalid")
    if not isinstance(etag, str) or not etag or _has_control_character(etag):
        raise BlobStreamError("Blob stat ETag is invalid")
    if not isinstance(uploaded_at, str):
        raise BlobStreamError("Blob stat uploadedAt is invalid")
    try:
        uploaded = datetime.fromisoformat(uploaded_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BlobStreamError("Blob stat uploadedAt is invalid") from exc
    if uploaded.tzinfo is None or uploaded.utcoffset() is None:
        raise BlobStreamError("Blob stat uploadedAt must include a timezone")
    content_type = payload.get("contentType")
    if content_type is not None and (
        not isinstance(content_type, str) or _has_control_character(content_type)
    ):
        raise BlobStreamError("Blob stat contentType is invalid")
    content_disposition = payload.get("contentDisposition", "")
    cache_control = payload.get("cacheControl", "")
    if not isinstance(content_disposition, str) or _has_control_character(content_disposition):
        raise BlobStreamError("Blob stat contentDisposition is invalid")
    if not isinstance(cache_control, str) or _has_control_character(cache_control):
        raise BlobStreamError("Blob stat cacheControl is invalid")
    return BlobStatResult(
        pathname=pathname,
        url=url,
        download_url=download_url,
        size=size,
        etag=etag,
        uploaded_at=uploaded,
        content_type=content_type,
        content_disposition=content_disposition,
        cache_control=cache_control,
    )


def _error(response: httpx.Response, pathname: str) -> Exception:
    code: str | None = None
    message = f"Blob request failed with HTTP {response.status_code}"
    try:
        payload = response.json()
        if isinstance(payload, dict):
            error = payload.get("error", payload)
            if isinstance(error, dict):
                code = cast(str | None, error.get("code"))
                message = str(error.get("message") or message)
    except Exception:
        pass
    if "contentType" in message and "is not allowed" in message:
        code = "content_type_not_allowed"
    if '"pathname"' in message and "does not match the token payload" in message:
        code = "client_token_pathname_mismatch"
    if "the file length cannot be greater than" in message:
        code = "file_too_large"
    if code in ("blob_not_found", "not_found"):
        return BlobNotFoundError(response=response)
    if code == "store_not_found":
        return BlobStoreNotFoundError(response=response)
    if code == "store_suspended":
        return BlobStoreSuspendedError(response=response)
    if code == "forbidden" or response.status_code in (401, 403):
        return BlobAccessError(response=response)
    if code == "content_type_not_allowed":
        return BlobContentTypeNotAllowedError(message, response=response)
    if code == "client_token_pathname_mismatch":
        return BlobPathnameMismatchError(message, response=response)
    if code == "file_too_large":
        return BlobFileTooLargeError(message, response=response)
    if code == "precondition_failed" or response.status_code == 412:
        return BlobPreconditionFailedError("Blob ETag precondition failed", response=response)
    if code == "bad_request":
        return BlobError(message or "Bad request", response=response)
    if code == "service_unavailable":
        return BlobServiceNotAvailable(response=response)
    if code == "rate_limited":
        retry_after = response.headers.get("retry-after", "")
        seconds = _parse_retry_after(retry_after)
        return BlobServiceRateLimited(seconds, response=response)
    return BlobUnknownError(response=response)


def _parse_retry_after(value: str) -> int | None:
    try:
        return max(0, int(value))
    except ValueError:
        pass
    try:
        retry_at = parsedate_to_datetime(value)
        now = datetime.now(retry_at.tzinfo)
    except (TypeError, ValueError, OverflowError):
        return None
    return max(0, math.ceil((retry_at - now).total_seconds()))


def _json_response(response: httpx.Response, message: str) -> dict[str, Any]:
    try:
        payload = response.json()
    except Exception as exc:
        raise BlobStreamError(message) from exc
    if not isinstance(payload, dict):
        raise BlobStreamError(message)
    return cast(dict[str, Any], payload)


class BlobApiClient:
    def __init__(
        self,
        *,
        base_url: str,
        transport: BaseTransport,
        credentials: Callable[[], Coroutine[None, None, BlobCredentials]],
    ) -> None:
        self.base_url = base_url
        self.transport = transport
        self.credentials = credentials
        self._store_id: str | None = None

    async def _auth(self) -> tuple[BlobCredentials, dict[str, str]]:
        credentials = _normalize_credentials(await self.credentials())
        if self._store_id is None:
            self._store_id = credentials.store_id
        elif self._store_id != credentials.store_id:
            raise BlobCredentialsError("Blob credential factory changed store ID")
        headers = {"x-api-version": "12"}
        if credentials.kind == "oidc":
            headers["x-vercel-blob-store-id"] = credentials.store_id
        return credentials, headers

    async def stat(self, pathname: str) -> BlobStatResult:
        credentials, headers = await self._auth()
        try:
            response = await self.transport.send(
                "GET",
                self.base_url,
                token=credentials.token,
                params={"url": pathname},
                headers=headers,
                read_response=ReadResponsePolicy.ALWAYS,
            )
        except httpx.HTTPError as exc:
            raise BlobUnknownError() from exc
        if not response.is_success:
            raise _error(response, pathname)
        return _parse_stat(_json_response(response, "Blob API returned malformed metadata"))

    async def read_range(
        self,
        stat: BlobStatResult,
        *,
        access: Access,
        start: int,
        end: int,
    ) -> bytes:
        credentials, _ = await self._auth()
        headers = {"range": f"bytes={start}-{end}", "if-match": stat.etag}
        token = credentials.token if access == "private" else None
        try:
            response = await self.transport.send(
                "GET",
                stat.url,
                token=token,
                headers=headers,
                follow_redirects=True,
                read_response=ReadResponsePolicy.ALWAYS,
            )
        except httpx.HTTPError as exc:
            raise BlobUnknownError() from exc
        if response.status_code == 404:
            raise BlobNotFoundError(response=response)
        if response.status_code == 412:
            raise BlobPreconditionFailedError("Blob ETag precondition failed", response=response)
        if response.status_code != 206:
            raise BlobStreamError("Blob delivery did not return a partial response")
        if response.headers.get("etag") != stat.etag:
            raise BlobPreconditionFailedError("Blob ETag precondition failed", response=response)
        match = _CONTENT_RANGE.fullmatch(response.headers.get("content-range", ""))
        if match is None or tuple(map(int, match.groups())) != (start, end, stat.size):
            raise BlobStreamError("Blob delivery returned invalid range metadata")
        expected = end - start + 1
        if len(response.content) != expected:
            raise BlobStreamError("Blob delivery returned an invalid byte count")
        return response.content

    async def put(
        self,
        pathname: str,
        body: bytes,
        *,
        access: Access,
        content_type: str | None,
        cache_control_max_age: timedelta | None,
    ) -> None:
        credentials, headers = await self._auth()
        headers.update(
            {
                "x-vercel-blob-access": access,
                "x-add-random-suffix": "0",
                "x-allow-overwrite": "1",
            }
        )
        if content_type is not None:
            headers["x-content-type"] = content_type
        if cache_control_max_age is not None:
            headers["x-cache-control-max-age"] = str(int(cache_control_max_age.total_seconds()))
        try:
            response = await self.transport.send(
                "PUT",
                self.base_url,
                token=credentials.token,
                params={"pathname": pathname},
                headers=headers,
                body=RawBody(body),
                read_response=ReadResponsePolicy.ALWAYS,
            )
        except httpx.HTTPError as exc:
            raise BlobUnknownError() from exc
        if not response.is_success:
            raise _error(response, pathname)
        etag = _json_response(response, "Blob publication response is invalid").get("etag")
        if not isinstance(etag, str) or not etag:
            raise BlobStreamError("Blob publication response is missing an ETag")

    async def remove(self, pathname: str) -> None:
        credentials, headers = await self._auth()
        try:
            response = await self.transport.send(
                "POST",
                f"{self.base_url.rstrip('/')}/delete",
                token=credentials.token,
                headers=headers,
                body=JSONBody({"urls": [pathname]}),
                read_response=ReadResponsePolicy.ALWAYS,
            )
        except httpx.HTTPError as exc:
            raise BlobUnknownError() from exc
        if not response.is_success:
            raise _error(response, pathname)
