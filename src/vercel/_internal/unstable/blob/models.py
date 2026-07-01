"""Domain state shared by the experimental Blob runtimes."""

import threading
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import TypeAlias

from vercel._internal.http import StreamingResponse
from vercel._internal.polyfills import StrEnum


class ScandirMode(StrEnum):
    """Controls whether Blob listings fold common prefixes."""

    FOLDED = "folded"
    EXPANDED = "expanded"


class PresignedOperation(StrEnum):
    """Blob operations that can be delegated through presigned URLs."""

    GET = "get"
    HEAD = "head"
    PUT = "put"
    DELETE = "delete"


@dataclass(frozen=True, slots=True)
class BlobStatResult:
    """Complete metadata for one Blob object."""

    pathname: str
    url: str
    download_url: str
    size: int
    etag: str
    uploaded_at: datetime
    content_type: str | None
    content_disposition: str
    cache_control: str


@dataclass(frozen=True, slots=True)
class BlobListItemState:
    """Snapshot metadata for one object returned by a listing page."""

    pathname: str
    url: str
    download_url: str
    size: int
    etag: str
    uploaded_at: datetime


@dataclass(frozen=True, slots=True)
class BlobPrefixState:
    """Common prefix returned by a folded listing page."""

    pathname: str

    def __post_init__(self) -> None:
        if not self.pathname.endswith("/"):
            raise ValueError("Blob prefix pathname must end with '/'")


BlobEntryState: TypeAlias = BlobListItemState | BlobPrefixState


@dataclass(frozen=True, slots=True)
class BlobPageState:
    """One page of Blob listing results."""

    entries: tuple[BlobEntryState, ...]
    cursor: str | None
    has_more: bool


@dataclass(frozen=True, slots=True)
class PresignedUrl:
    """A presigned Blob URL and the constraints a caller must honor."""

    url: str
    operation: PresignedOperation
    expires_at: datetime
    required_headers: Mapping[str, str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "required_headers", MappingProxyType(dict(self.required_headers)))


@dataclass(frozen=True, slots=True)
class MultipartUploadState:
    """Backend identifiers for an in-progress multipart upload."""

    pathname: str
    upload_id: str
    key: str


@dataclass(frozen=True, slots=True)
class MultipartPartState:
    """Metadata returned after uploading one multipart part."""

    part_number: int
    etag: str


class BlobRangeResponse:
    """Validated delivery range that owns its streaming HTTP response."""

    def __init__(
        self,
        response: StreamingResponse | None,
        *,
        start: int,
        end: int,
        total: int,
    ) -> None:
        self._response = response
        self._start = start
        self._end = end
        self._total = total
        self._response_lock = threading.Lock()

    @property
    def start(self) -> int:
        """Inclusive byte offset of the response."""
        return self._start

    @property
    def end(self) -> int:
        """Inclusive byte offset of the response."""
        return self._end

    @property
    def total(self) -> int:
        """Total size of the Blob object the range belongs to."""
        return self._total

    def __aiter__(self) -> AsyncIterator[bytes]:
        with self._response_lock:
            response = self._response
        if response is None:
            return _empty_byte_iterator()
        return response

    def _take_response(self) -> StreamingResponse | None:
        with self._response_lock:
            response = self._response
            self._response = None
            return response

    async def aclose(self) -> None:
        """Close the owned streaming HTTP response, if one remains open."""
        response = self._take_response()
        if response is not None:
            await response.aclose()


async def _empty_byte_iterator() -> AsyncIterator[bytes]:
    if False:
        yield b""


@dataclass(frozen=True, slots=True)
class _FileMode:
    value: str
    binary: bool
    reading: bool
    writing: bool
    exclusive: bool
    appending: bool
    updating: bool
    truncating: bool
    requires_staging: bool


_VALID_FILE_MODES = frozenset(
    {
        "r",
        "rb",
        "w",
        "wb",
        "x",
        "xb",
        "a",
        "ab",
        "r+",
        "r+b",
        "rb+",
        "w+",
        "w+b",
        "wb+",
        "x+",
        "x+b",
        "xb+",
        "a+",
        "a+b",
        "ab+",
    }
)


def _parse_file_mode(mode: str) -> _FileMode:
    if mode not in _VALID_FILE_MODES:
        raise ValueError(f"invalid mode: {mode!r}")

    operation = mode[0]
    updating = "+" in mode
    appending = operation == "a"
    return _FileMode(
        value=mode,
        binary="b" in mode,
        reading=operation == "r" or updating,
        writing=operation != "r" or updating,
        exclusive=operation == "x",
        appending=appending,
        updating=updating,
        truncating=operation == "w",
        requires_staging=appending or updating,
    )
