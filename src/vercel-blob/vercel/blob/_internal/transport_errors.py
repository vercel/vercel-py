"""Map HTTP client exceptions to the Blob errors raised in their place."""

from __future__ import annotations

import sys

import httpx2 as httpx

from vercel.blob.errors import BlobError, BlobStreamError, BlobTimeoutError, BlobUnknownError


def _httpx_types(name: str) -> tuple[type[BaseException], ...]:
    types: list[type[BaseException]] = [getattr(httpx, name)]
    # Sessions may use a legacy HTTPX client, which raises its own exception family.
    legacy = getattr(sys.modules.get("httpx"), name, None)
    if isinstance(legacy, type) and issubclass(legacy, BaseException):
        types.append(legacy)
    return tuple(types)


def transport_failure(exc: BaseException) -> BlobUnknownError:
    """Map a failed HTTP exchange to the Blob error raised in its place."""
    if isinstance(exc, _httpx_types("TimeoutException")):
        return BlobTimeoutError()
    return BlobUnknownError()


def body_read_failure(exc: BaseException) -> BlobError:
    """Map an HTTP failure while reading a response body."""
    if isinstance(exc, _httpx_types("DecodingError")):
        return BlobStreamError("Blob response body could not be decoded")
    return transport_failure(exc)
