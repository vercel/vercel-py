"""Wire protocol encoding, response parsing, and error mapping for Vercel Blob."""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import datetime
from email.utils import parsedate_to_datetime

import httpx2 as httpx

from vercel.blob.errors import (
    BlobAccessError,
    BlobContentTypeNotAllowedError,
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
from vercel.blob.models import DownloadMetadata, HeadResult, PutResult


def parse_retry_after(value: str) -> int | None:
    try:
        return max(0, int(value))
    except ValueError:
        pass
    try:
        retry_at = parsedate_to_datetime(value)
        now = datetime.now(retry_at.tzinfo)
        return max(0, math.ceil((retry_at - now).total_seconds()))
    except (TypeError, ValueError, OverflowError):
        return None


def map_http_error(response: httpx.Response) -> Exception:
    code: str | None = None
    message = f"Blob request failed with HTTP {response.status_code}"

    try:
        payload = response.json()
        if isinstance(payload, dict):
            err_obj = payload.get("error", payload)
            if isinstance(err_obj, dict):
                raw_code = err_obj.get("code")
                if isinstance(raw_code, str):
                    code = raw_code
                msg = err_obj.get("message")
                if isinstance(msg, str):
                    message = msg
    except Exception:
        pass

    if "contentType" in message and "is not allowed" in message:
        code = "content_type_not_allowed"
    if '"pathname"' in message and "does not match the token payload" in message:
        code = "client_token_pathname_mismatch"
    if "the file length cannot be greater than" in message:
        code = "file_too_large"

    # Map explicit backend error codes first
    if code == "store_not_found":
        return BlobStoreNotFoundError(message, status_code=response.status_code, code=code)
    if code == "store_suspended":
        return BlobStoreSuspendedError(message, status_code=response.status_code, code=code)
    if code in ("blob_not_found", "not_found"):
        return BlobNotFoundError(message, status_code=response.status_code, code=code)
    if code == "forbidden":
        return BlobAccessError(message, status_code=response.status_code, code=code)
    if code == "content_type_not_allowed":
        return BlobContentTypeNotAllowedError(message, status_code=response.status_code, code=code)
    if code == "client_token_pathname_mismatch":
        return BlobPathnameMismatchError(message, status_code=response.status_code, code=code)
    if code == "file_too_large":
        return BlobFileTooLargeError(message, status_code=response.status_code, code=code)
    if code == "precondition_failed":
        return BlobPreconditionFailedError(message, status_code=response.status_code, code=code)
    if code == "service_unavailable":
        return BlobServiceNotAvailable(message, status_code=response.status_code, code=code)
    if code == "rate_limited":
        retry_after = response.headers.get("retry-after", "")
        seconds = parse_retry_after(retry_after)
        return BlobServiceRateLimited(seconds, status_code=response.status_code, code=code)

    # Status code fallbacks
    if response.status_code == 404:
        return BlobNotFoundError(message, status_code=404, code="blob_not_found")
    if response.status_code in (401, 403):
        return BlobAccessError(message, status_code=response.status_code, code="forbidden")
    if response.status_code == 412:
        return BlobPreconditionFailedError(message, status_code=412, code="precondition_failed")
    if response.status_code == 429:
        retry_after = response.headers.get("retry-after", "")
        seconds = parse_retry_after(retry_after)
        return BlobServiceRateLimited(seconds, status_code=429, code="rate_limited")
    if response.status_code == 503:
        return BlobServiceNotAvailable(message, status_code=503, code="service_unavailable")
    if response.status_code == 400:
        return BlobError(message, status_code=400, code="bad_request")

    return BlobUnknownError(message, status_code=response.status_code, code=code)


def parse_put_response(response: httpx.Response) -> PutResult:
    try:
        data = response.json()
    except Exception as exc:
        raise BlobStreamError("Blob PUT response is not valid JSON") from exc

    if not isinstance(data, dict):
        raise BlobStreamError("Blob PUT response must be a JSON object")

    url = data.get("url")
    if not isinstance(url, str):
        raise BlobStreamError("Blob PUT response 'url' must be a string")

    download_url = data.get("downloadUrl")
    if not isinstance(download_url, str):
        raise BlobStreamError("Blob PUT response 'downloadUrl' must be a string")

    pathname = data.get("pathname")
    if not isinstance(pathname, str):
        raise BlobStreamError("Blob PUT response 'pathname' must be a string")

    content_type = data.get("contentType")
    if not isinstance(content_type, str):
        raise BlobStreamError("Blob PUT response 'contentType' must be a string")

    content_disposition = data.get("contentDisposition")
    if not isinstance(content_disposition, str):
        raise BlobStreamError("Blob PUT response 'contentDisposition' must be a string")

    etag = data.get("etag")
    if not isinstance(etag, str):
        raise BlobStreamError("Blob PUT response 'etag' must be a string")

    return PutResult(
        url=url,
        download_url=download_url,
        pathname=pathname,
        content_type=content_type,
        content_disposition=content_disposition,
        etag=etag,
    )


def parse_head_response(response: httpx.Response) -> HeadResult:
    try:
        data = response.json()
    except Exception as exc:
        raise BlobStreamError("Blob HEAD response is not valid JSON") from exc

    if not isinstance(data, dict):
        raise BlobStreamError("Blob HEAD response must be a JSON object")

    url = data.get("url")
    if not isinstance(url, str):
        raise BlobStreamError("Blob HEAD response 'url' must be a string")

    download_url = data.get("downloadUrl")
    if not isinstance(download_url, str):
        raise BlobStreamError("Blob HEAD response 'downloadUrl' must be a string")

    pathname = data.get("pathname")
    if not isinstance(pathname, str):
        raise BlobStreamError("Blob HEAD response 'pathname' must be a string")

    size = data.get("size")
    if type(size) is not int or size < 0:
        raise BlobStreamError("Blob HEAD response has invalid size field")

    etag = data.get("etag")
    if not isinstance(etag, str):
        raise BlobStreamError("Blob HEAD response 'etag' must be a string")

    uploaded_at_raw = data.get("uploadedAt")
    if not isinstance(uploaded_at_raw, str):
        raise BlobStreamError("Blob HEAD response 'uploadedAt' must be a string")

    try:
        uploaded_at = datetime.fromisoformat(uploaded_at_raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BlobStreamError(f"Invalid uploadedAt date: {uploaded_at_raw!r}") from exc

    content_type = data.get("contentType")
    if content_type is not None and not isinstance(content_type, str):
        raise BlobStreamError("Blob HEAD response 'contentType' must be a string or null")

    content_disposition = data.get("contentDisposition", "")
    if not isinstance(content_disposition, str):
        raise BlobStreamError("Blob HEAD response 'contentDisposition' must be a string")

    cache_control = data.get("cacheControl", "")
    if not isinstance(cache_control, str):
        raise BlobStreamError("Blob HEAD response 'cacheControl' must be a string")

    return HeadResult(
        url=url,
        download_url=download_url,
        pathname=pathname,
        size=size,
        etag=etag,
        uploaded_at=uploaded_at,
        content_type=content_type,
        content_disposition=content_disposition,
        cache_control=cache_control,
    )


def parse_download_metadata(url: str, response: httpx.Response) -> DownloadMetadata:
    headers: Mapping[str, str] = dict(response.headers)
    status_code = response.status_code

    size: int | None = None
    if "content-length" in headers:
        raw_cl = headers["content-length"]
        try:
            parsed_size = int(raw_cl)
            if parsed_size < 0:
                raise ValueError
            size = parsed_size
        except ValueError as exc:
            raise BlobStreamError(
                f"Blob GET response has invalid content-length header: {raw_cl!r}"
            ) from exc

    content_type = headers.get("content-type")
    content_disposition = headers.get("content-disposition")
    cache_control = headers.get("cache-control")
    etag = headers.get("etag")

    last_modified: datetime | None = None
    if "last-modified" in headers:
        raw_lm = headers["last-modified"]
        try:
            last_modified = parsedate_to_datetime(raw_lm)
        except (TypeError, ValueError, OverflowError) as exc:
            raise BlobStreamError(
                f"Blob GET response has invalid last-modified header: {raw_lm!r}"
            ) from exc

    return DownloadMetadata(
        url=url,
        status_code=status_code,
        size=size,
        content_type=content_type,
        content_disposition=content_disposition,
        cache_control=cache_control,
        etag=etag,
        last_modified=last_modified,
        headers=headers,
    )


def validate_and_parse_download_response(url: str, response: httpx.Response) -> DownloadMetadata:
    """Validate status and parse response headers into DownloadMetadata."""
    if not response.is_success:
        raise map_http_error(response)
    return parse_download_metadata(url, response)
