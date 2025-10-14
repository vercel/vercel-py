from __future__ import annotations

import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Iterable, Protocol
import asyncio

from .errors import BlobError


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


def get_api_url(pathname: str = "") -> str:
    base_url = os.getenv("VERCEL_BLOB_API_URL") or os.getenv(
        "NEXT_PUBLIC_VERCEL_BLOB_API_URL"
    )
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


def get_token_from_options_or_env(options: dict[str, Any] | None = None) -> str:
    if options and "token" in options and options["token"]:
        return str(options["token"])
    env_token = os.getenv("BLOB_READ_WRITE_TOKEN")
    if env_token:
        return env_token
    raise BlobError(
        "No token found. Either configure the `BLOB_READ_WRITE_TOKEN` environment variable, or pass a `token` option to your calls."
    )


def extract_store_id_from_token(token: str) -> str:
    try:
        parts = token.split("_")
        return parts[3] if len(parts) > 3 else ""
    except Exception:
        return ""


def validate_pathname(pathname: str) -> None:
    if not pathname:
        raise BlobError("pathname is required")
    if len(pathname) > MAXIMUM_PATHNAME_LENGTH:
        raise BlobError(
            f"pathname is too long, maximum length is {MAXIMUM_PATHNAME_LENGTH}"
        )
    for invalid in DISALLOWED_PATHNAME_CHARACTERS:
        if invalid in pathname:
            raise BlobError(
                f'pathname cannot contain "{invalid}", please encode it if needed'
            )


def require_public_access(options: dict[str, Any]) -> None:
    if options.get("access") != "public":
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


OnUploadProgressCallback = Callable[[UploadProgressEvent], None]


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
                UploadProgressEvent(
                    loaded=self._loaded, total=total, percentage=percentage
                )
            )

    async def __aiter__(self):  # type: ignore[override]
        for chunk in self:
            yield chunk
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
        from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

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
