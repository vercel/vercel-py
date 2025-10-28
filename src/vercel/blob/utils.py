from __future__ import annotations

import asyncio
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Iterable, Protocol, Awaitable, TypedDict

from .errors import BlobError, BlobNoTokenProvidedError

DEFAULT_VERCEL_BLOB_API_URL = "https://vercel.com/api/blob"
MAXIMUM_PATHNAME_LENGTH = 950
DISALLOWED_PATHNAME_CHARACTERS = ["//"]


def debug(message: str, *args: Any) -> None:
    try:
        debug_env = os.getenv("DEBUG", "") or os.getenv("NEXT_PUBLIC_DEBUG", "")
        if "blob" in debug_env:
            print(f"vercel-blob: {message}", *args)
    except Exception:
        pass


def is_url(value: str) -> bool:
    return value.startswith(("http://", "https://"))


def normalize_path(p: str | os.PathLike) -> str:
    s = str(p)
    # prevent accidental double slashes and backslashes
    s = s.replace("\\", "/")
    while "//" in s:
        s = s.replace("//", "/")
    # disallow empty or root-only
    if not s or s == "/":
        raise ValueError("path must not be empty or '/'")
    # normalize leading slash away: 'a/b' not '/a/b'
    if s.startswith("/"):
        s = s[1:]
    return s


def build_cache_control(cache_control: str | None, max_age: int | None) -> str | None:
    if cache_control:
        return cache_control
    if max_age is not None:
        return f"max-age={int(max_age)}"
    return None


def get_api_url(pathname: str = "") -> str:
    base_url = os.getenv("VERCEL_BLOB_API_URL") or os.getenv("NEXT_PUBLIC_VERCEL_BLOB_API_URL")
    return f"{base_url or DEFAULT_VERCEL_BLOB_API_URL}{pathname}"


def get_api_version() -> str:
    override = os.getenv("VERCEL_BLOB_API_VERSION_OVERRIDE") or os.getenv(
        "NEXT_PUBLIC_VERCEL_BLOB_API_VERSION_OVERRIDE"
    )
    # Match TS constant 11 unless overridden
    return str(override or 11)


def get_retries() -> int:
    retries = os.getenv("VERCEL_BLOB_RETRIES")
    try:
        return int(retries) if retries is not None else 10
    except Exception:
        return 10


def should_use_x_content_length() -> bool:
    return os.getenv("VERCEL_BLOB_USE_X_CONTENT_LENGTH") == "1"


def get_proxy_through_alternative_api_header_from_env() -> dict[str, str]:
    headers: dict[str, str] = {}
    value = os.getenv("VERCEL_BLOB_PROXY_THROUGH_ALTERNATIVE_API")
    if value is not None:
        headers["x-proxy-through-alternative-api"] = value
    else:
        value = os.getenv("NEXT_PUBLIC_VERCEL_BLOB_PROXY_THROUGH_ALTERNATIVE_API")
        if value is not None:
            headers["x-proxy-through-alternative-api"] = value
    return headers


def extract_store_id_from_token(token: str) -> str:
    try:
        parts = token.split("_")
        return parts[3] if len(parts) > 3 else ""
    except Exception:
        return ""


def validate_path(path: str) -> None:
    if not path:
        raise BlobError("path is required")
    if len(path) > MAXIMUM_PATHNAME_LENGTH:
        raise BlobError(f"path is too long, maximum length is {MAXIMUM_PATHNAME_LENGTH}")
    for invalid in DISALLOWED_PATHNAME_CHARACTERS:
        if invalid in path:
            raise BlobError(f'path cannot contain "{invalid}", please encode it if needed')


def require_public_access(access: str) -> None:
    if access != "public":
        raise BlobError('access must be "public"')


def compute_body_length(body: Any) -> int:
    if body is None:
        return 0
    # str -> utf-8 byte length
    if isinstance(body, str):
        return len(body.encode("utf-8"))
    # bytes-like
    if isinstance(body, (bytes, bytearray, memoryview)):
        return len(body)
    # file-like object with seek/tell
    if hasattr(body, "read"):
        try:
            pos = body.tell()  # type: ignore[attr-defined]
            body.seek(0, 2)  # type: ignore[attr-defined]
            end = body.tell()  # type: ignore[attr-defined]
            body.seek(pos)  # type: ignore[attr-defined]
            return int(end - pos)
        except Exception:
            return 0
    # iterable/generator unknown length
    return 0


# Progress
@dataclass
class UploadProgressEvent:
    loaded: int
    total: int
    percentage: float


OnUploadProgressCallback = (
    Callable[[UploadProgressEvent], None] | Callable[[UploadProgressEvent], Awaitable[None]]
)


class SupportsRead(Protocol):
    def read(self, size: int = -1) -> bytes:  # pragma: no cover - Protocol
        ...


class StreamingBodyWithProgress:
    """Wrap a bytes/str/file-like or iterable body to provide progress callbacks.

    This wrapper yields bytes in chunks and calls the provided callback with
    updated progress. It also computes total length when possible.
    """

    def __init__(
        self,
        body: bytes | bytearray | memoryview | str | SupportsRead | Iterable[bytes],
        on_progress: OnUploadProgressCallback | None,
        chunk_size: int = 64 * 1024,
        total: int | None = None,
    ) -> None:
        self._source = body
        self._on_progress = on_progress
        self._chunk_size = max(1024, chunk_size)
        self._loaded = 0
        self._total = total if total is not None else compute_body_length(body)

    def __iter__(self) -> Iterable[bytes]:
        if isinstance(self._source, str):
            data = self._source.encode("utf-8")
            yield from self._yield_bytes(data)
            return
        if isinstance(self._source, (bytes, bytearray, memoryview)):
            yield from self._yield_bytes(bytes(self._source))
            return
        if hasattr(self._source, "read"):
            # file-like
            while True:
                chunk = self._source.read(self._chunk_size)  # type: ignore[attr-defined]
                if not chunk:
                    break
                if not isinstance(chunk, (bytes, bytearray, memoryview)):
                    chunk = bytes(chunk)
                self._loaded += len(chunk)
                self._emit_progress()
                yield bytes(chunk)
            return
        # assume iterable of bytes
        for chunk in self._source:  # type: ignore[assignment]
            if not isinstance(chunk, (bytes, bytearray, memoryview)):
                chunk = bytes(chunk)
            self._loaded += len(chunk)
            self._emit_progress()
            yield bytes(chunk)

    def _yield_bytes(self, data: bytes) -> Iterable[bytes]:
        view = memoryview(data)
        offset = 0
        while offset < len(view):
            end = min(offset + self._chunk_size, len(view))
            chunk = view[offset:end]
            offset = end
            self._loaded += len(chunk)
            self._emit_progress()
            yield chunk.tobytes()

    def _emit_progress(self) -> None:
        if self._on_progress:
            total = self._total if self._total else self._loaded
            percentage = round((self._loaded / total) * 100, 2) if total else 0.0
            self._on_progress(
                UploadProgressEvent(loaded=self._loaded, total=total, percentage=percentage)
            )

    async def _emit_progress_async(self) -> None:
        if self._on_progress:
            total = self._total if self._total else self._loaded
            percentage = round((self._loaded / total) * 100, 2) if total else 0.0
            result = self._on_progress(
                UploadProgressEvent(loaded=self._loaded, total=total, percentage=percentage)
            )
            # Check if the callback is async
            if asyncio.iscoroutine(result):
                await result

    async def __aiter__(self):  # type: ignore[override]
        # Async version that properly handles async callbacks
        if isinstance(self._source, str):
            data = self._source.encode("utf-8")
            async for chunk in self._yield_bytes_async(data):
                yield chunk
            return
        if isinstance(self._source, (bytes, bytearray, memoryview)):
            async for chunk in self._yield_bytes_async(bytes(self._source)):
                yield chunk
            return
        if hasattr(self._source, "read"):
            # file-like
            while True:
                chunk = self._source.read(self._chunk_size)  # type: ignore[attr-defined]
                if not chunk:
                    break
                if not isinstance(chunk, (bytes, bytearray, memoryview)):
                    chunk = bytes(chunk)
                self._loaded += len(chunk)
                await self._emit_progress_async()
                yield bytes(chunk)
                await asyncio.sleep(0)
            return
        # assume iterable of bytes
        for chunk in self._source:  # type: ignore[assignment]
            if not isinstance(chunk, (bytes, bytearray, memoryview)):
                chunk = bytes(chunk)
            self._loaded += len(chunk)
            await self._emit_progress_async()
            yield bytes(chunk)
            await asyncio.sleep(0)

    async def _yield_bytes_async(self, data: bytes):
        view = memoryview(data)
        offset = 0
        while offset < len(view):
            end = min(offset + self._chunk_size, len(view))
            chunk = view[offset:end]
            offset = end
            self._loaded += len(chunk)
            await self._emit_progress_async()
            yield chunk.tobytes()
            await asyncio.sleep(0)


def make_request_id(store_id: str) -> str:
    return f"{store_id}:{int(time.time() * 1000)}:{uuid.uuid4().hex[:8]}"


def parse_rfc7231_retry_after(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(value)
    except Exception:
        return None


def parse_datetime(value: str) -> datetime:
    # API returns ISO timestamps; best-effort parsing
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")


def get_download_url(blob_url: str) -> str:
    try:
        from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

        parsed = urlparse(blob_url)
        q = dict(parse_qsl(parsed.query))
        q["download"] = "1"
        new_query = urlencode(q)
        return urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                new_query,
                parsed.fragment,
            )
        )
    except Exception:
        # Fallback: naive append
        sep = "&" if "?" in blob_url else "?"
        return f"{blob_url}{sep}download=1"


# TypedDict with real HTTP header keys. Use functional syntax to allow hyphens.
PutHeaders = TypedDict(
    "PutHeaders",
    {
        "x-cache-control-max-age": str,
        "x-add-random-suffix": str,
        "x-allow-overwrite": str,
        "x-content-type": str,
    },
    total=False,
)


def create_put_headers(
    content_type: str | None = None,
    add_random_suffix: bool | None = None,
    allow_overwrite: bool | None = None,
    cache_control_max_age: int | None = None,
) -> PutHeaders:
    headers: PutHeaders = {}
    if content_type:
        headers["x-content-type"] = content_type
    if add_random_suffix is not None:
        headers["x-add-random-suffix"] = "1" if add_random_suffix else "0"
    if allow_overwrite is not None:
        headers["x-allow-overwrite"] = "1" if allow_overwrite else "0"
    if cache_control_max_age is not None:
        headers["x-cache-control-max-age"] = str(cache_control_max_age)
    return headers


def ensure_token(token: str | None) -> str:
    token = token or os.getenv("BLOB_READ_WRITE_TOKEN") or os.getenv("VERCEL_BLOB_READ_WRITE_TOKEN")
    if not token:
        raise BlobNoTokenProvidedError()
    return token
