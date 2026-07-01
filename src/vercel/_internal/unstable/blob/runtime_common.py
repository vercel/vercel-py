"""Runtime entry handles for unstable Blob listings."""

from datetime import datetime, timedelta
from typing import Literal, TypeAlias, cast
from urllib.parse import urlsplit

import anyio

from vercel._internal.blob.types import Access
from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.unstable.blob.duration import (
    DurationInput,
    parse_required_duration_seconds,
)
from vercel._internal.unstable.blob.errors import BlobStreamError
from vercel._internal.unstable.blob.models import (
    BlobListItemState,
    BlobPrefixState,
    BlobStatResult,
    PresignedOperation,
    PresignedUrl,
)
from vercel._internal.unstable.blob.service import BlobService

_ReadOperation: TypeAlias = PresignedOperation | Literal["get", "head"]


def _entry_name(pathname: str) -> str:
    return pathname.rstrip("/").rsplit("/", 1)[-1]


def _delivery_access(url: str) -> Access:
    hostname = urlsplit(url).hostname or ""
    suffix = ".blob.vercel-storage.com"
    if not hostname.endswith(suffix):
        raise BlobStreamError("Blob list metadata has an invalid delivery access URL")
    labels = hostname[: -len(suffix)].split(".")
    if len(labels) < 2 or labels[-1] not in ("public", "private"):
        raise BlobStreamError("Blob list metadata has an invalid delivery access URL")
    return cast(Access, labels[-1])


class _BlobObjectEntryBase:
    __slots__ = ("_payload", "_service", "_stat_result")

    def __init__(self, payload: BlobListItemState, service: BlobService) -> None:
        self._payload = payload
        self._service = service
        self._stat_result: BlobStatResult | None = None

    @property
    def name(self) -> str:
        """Final pathname component for this listed object."""
        return _entry_name(self._payload.pathname)

    @property
    def path(self) -> str:
        """Store-relative pathname for this listed object."""
        return self._payload.pathname

    @property
    def url(self) -> str:
        """Browser-facing Blob URL from listing metadata."""
        return self._payload.url

    @property
    def download_url(self) -> str:
        """Download URL from listing metadata."""
        return self._payload.download_url

    @property
    def size(self) -> int:
        """Object size from listing metadata, in bytes."""
        return self._payload.size

    @property
    def uploaded_at(self) -> datetime:
        """Upload timestamp from listing metadata."""
        return self._payload.uploaded_at

    @property
    def etag(self) -> str:
        """Object ETag from listing metadata."""
        return self._payload.etag

    def is_object(self) -> Literal[True]:
        """Return whether this entry represents an object."""
        return True

    def is_prefix(self) -> Literal[False]:
        """Return whether this entry represents a common prefix."""
        return False

    @staticmethod
    def _normalize_read_operation(operation: _ReadOperation) -> PresignedOperation:
        try:
            normalized = PresignedOperation(operation)
        except ValueError:
            raise ValueError("Blob object entry presigning accepts only get and head") from None
        if normalized not in (PresignedOperation.GET, PresignedOperation.HEAD):
            raise ValueError("Blob object entry presigning accepts only get and head")
        return normalized


class _BlobStatOperation:
    __slots__ = ("done", "error")

    def __init__(self) -> None:
        self.done = anyio.Event()
        self.error: BaseException | None = None


class BlobObjectEntry(_BlobObjectEntryBase):
    """Asynchronous handle for one object observed by a listing."""

    __slots__ = ("_stat_operation",)

    def __init__(self, payload: BlobListItemState, service: BlobService) -> None:
        super().__init__(payload, service)
        self._stat_operation: _BlobStatOperation | None = None

    async def stat(self) -> BlobStatResult:
        """Return complete metadata for this object, fetching it once.

        Returns:
            Complete metadata for the listed object.
        """
        if self._stat_result is not None:
            return self._stat_result
        operation = self._stat_operation
        if operation is None:
            operation = _BlobStatOperation()
            self._stat_operation = operation
            try:
                self._stat_result = await self._service.stat(self.path)
            except BaseException as error:
                if not isinstance(error, anyio.get_cancelled_exc_class()) and isinstance(
                    error, Exception
                ):
                    operation.error = error
                raise
            finally:
                self._stat_operation = None
                operation.done.set()
            return self._stat_result

        await operation.done.wait()
        if operation.error is not None:
            raise operation.error
        # The owner task either populated _stat_result, recorded an ordinary
        # Exception above, or was cancelled/interrupted before publishing either.
        # In the last case this waiter becomes the next owner and retries.
        if self._stat_result is None:
            return await self.stat()
        # _stat_result can change while this task waits, even though this caller
        # observed None before awaiting operation.done.
        return self._stat_result

    async def presign(
        self,
        *,
        operation: _ReadOperation = PresignedOperation.GET,
        expires_in: DurationInput = timedelta(hours=1),
    ) -> PresignedUrl:
        """Sign a pathname-scoped read; replacement may change the served bytes."""
        normalized_operation = self._normalize_read_operation(operation)
        return await self._service.presign(
            self.path,
            operation=normalized_operation,
            access=_delivery_access(self.url),
            expires_in=parse_required_duration_seconds(expires_in, name="expires_in"),
        )


class SyncBlobObjectEntry(_BlobObjectEntryBase):
    """Synchronous handle for one object observed by a listing."""

    def stat(self) -> BlobStatResult:
        """Return complete metadata for this object, fetching it once.

        Returns:
            Complete metadata for the listed object.
        """
        if self._stat_result is None:
            self._stat_result = iter_coroutine(self._service.stat(self.path))
        return self._stat_result

    def presign(
        self,
        *,
        operation: _ReadOperation = PresignedOperation.GET,
        expires_in: DurationInput = timedelta(hours=1),
    ) -> PresignedUrl:
        """Sign a pathname-scoped read; replacement may change the served bytes."""
        normalized_operation = self._normalize_read_operation(operation)
        return iter_coroutine(
            self._service.presign(
                self.path,
                operation=normalized_operation,
                access=_delivery_access(self.url),
                expires_in=parse_required_duration_seconds(expires_in, name="expires_in"),
            )
        )


class _BlobPrefixEntryBase:
    __slots__ = ("_payload", "_service")

    def __init__(self, payload: BlobPrefixState, service: BlobService) -> None:
        self._payload = payload
        self._service = service

    @property
    def name(self) -> str:
        """Final pathname component for this common prefix."""
        return _entry_name(self._payload.pathname)

    @property
    def path(self) -> str:
        """Store-relative prefix pathname."""
        return self._payload.pathname

    def is_object(self) -> Literal[False]:
        """Return whether this entry represents an object."""
        return False

    def is_prefix(self) -> Literal[True]:
        """Return whether this entry represents a common prefix."""
        return True


class BlobPrefixEntry(_BlobPrefixEntryBase):
    """Asynchronous handle for one common prefix observed by a listing."""


class SyncBlobPrefixEntry(_BlobPrefixEntryBase):
    """Synchronous handle for one common prefix observed by a listing."""


BlobEntry: TypeAlias = BlobObjectEntry | BlobPrefixEntry
SyncBlobEntry: TypeAlias = SyncBlobObjectEntry | SyncBlobPrefixEntry
