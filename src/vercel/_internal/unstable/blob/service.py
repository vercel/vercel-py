"""Neutral orchestration for unstable Blob operations."""

import os
import re
from collections.abc import AsyncIterator, Callable, Sequence
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Literal, TypeAlias, overload

from vercel._internal.blob.types import Access
from vercel._internal.byte_stream import StagingFileRuntime
from vercel._internal.unstable.blob.api_client import BlobApiClient
from vercel._internal.unstable.blob.errors import (
    BlobCredentialsError,
    BlobNotFoundError,
    BlobRecursiveDeleteError,
    BlobStreamError,
)
from vercel._internal.unstable.blob.models import (
    BlobEntryState,
    BlobPageState,
    BlobStatResult,
    PresignedOperation,
    PresignedUrl,
    ScandirMode,
    _FileMode,
)
from vercel._internal.unstable.blob.options import (
    BlobServiceOptions,
    _AsyncBlobCredentialsResolver,
    _BlobCredentialsResolver,
    _SyncBlobCredentialsResolver,
)
from vercel._internal.unstable.blob.reader import BlobReaderState
from vercel._internal.unstable.blob.writer import BlobWriterState, create_writer_state

if TYPE_CHECKING:
    from vercel._internal.unstable.session import _BaseSdkSession

StrPath: TypeAlias = str | os.PathLike[str]

_MAX_PATHNAME_LENGTH = 950
_DELETE_BATCH_SIZE = 1000
_MAX_PRESIGN_AGE = timedelta(days=7)
_URL_PREFIX = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")


def _continuation_cursor(page: BlobPageState, seen: set[str]) -> str | None:
    if not page.has_more:
        return None
    cursor = page.cursor
    if not cursor or cursor in seen:
        raise BlobStreamError("Blob listing returned an invalid continuation cursor")
    seen.add(cursor)
    return cursor


class BlobService:
    """Async-only Blob domain orchestration returning neutral state."""

    def __init__(
        self,
        *,
        api_client: BlobApiClient,
        options: BlobServiceOptions,
        ensure_open: Callable[[], None],
        staging_file_runtime: StagingFileRuntime,
    ) -> None:
        self._api_client = api_client
        self._options = options
        self._ensure_open = ensure_open
        self._staging_file_runtime = staging_file_runtime

    @property
    def api_client(self) -> BlobApiClient:
        """Return the wire client used by this service."""
        return self._api_client

    @property
    def options(self) -> BlobServiceOptions:
        """Return the immutable options that configure this service."""
        return self._options

    @property
    def staging_file_runtime(self) -> StagingFileRuntime:
        """Return the session-owned runtime used for staged writes."""
        return self._staging_file_runtime

    @staticmethod
    def normalize_path(pathname: StrPath, *, allow_empty: bool = False) -> str:
        """Normalize one caller pathname into the store-relative wire form."""
        value = os.fspath(pathname)
        if not isinstance(value, str):
            raise TypeError("pathname must be a string path")
        value = value.replace("\\", "/")
        if value.startswith("//"):
            raise ValueError("pathname must not have a double root")
        if value.startswith("/"):
            value = value[1:]
        if not value:
            if allow_empty:
                return ""
            raise ValueError("pathname must not be empty or root")
        if _URL_PREFIX.match(value):
            raise ValueError("pathname must be store-relative, not a URL")
        if len(value) > _MAX_PATHNAME_LENGTH:
            raise ValueError(f"pathname must not exceed {_MAX_PATHNAME_LENGTH} characters")
        return value

    @classmethod
    def _normalize_prefix(cls, pathname: StrPath) -> str:
        normalized = cls.normalize_path(pathname).rstrip("/") + "/"
        if len(normalized) > _MAX_PATHNAME_LENGTH:
            raise ValueError(f"pathname must not exceed {_MAX_PATHNAME_LENGTH} characters")
        return normalized

    async def stat(self, pathname: StrPath) -> BlobStatResult:
        """Fetch complete metadata for one Blob object.

        Args:
            pathname: Store-relative object pathname.

        Returns:
            Complete object metadata from the Blob API.
        """
        normalized = self.normalize_path(pathname)
        self._ensure_open()
        return await self._api_client.stat(normalized)

    async def open_reader(self, pathname: StrPath, *, access: Access) -> BlobReaderState:
        """Create an ETag-pinned reader state for one Blob object.

        Args:
            pathname: Store-relative object pathname.
            access: Delivery access to use for range reads.

        Returns:
            Runtime-neutral reader state.
        """
        normalized = self.normalize_path(pathname)
        if normalized.endswith("/"):
            raise ValueError("Blob readers cannot open a trailing slash pathname")
        if access not in ("public", "private"):
            raise ValueError('access must be "public" or "private"')
        self._ensure_open()
        stat = await self._api_client.stat(normalized)
        return BlobReaderState(
            stat=stat,
            api_client=self._api_client,
            access=access,
            read_buffer_size=self._options.read_buffer_size,
            ensure_open=self._ensure_open,
        )

    async def open_writer(
        self,
        pathname: StrPath,
        *,
        mode: _FileMode,
        access: Access,
        content_type: str | None = None,
        cache_control_max_age: timedelta | None = None,
    ) -> BlobWriterState:
        """Create a transactional writer state for one Blob object.

        Args:
            pathname: Store-relative object pathname.
            mode: Parsed Python file mode for the writer.
            access: Delivery access to publish with the object.
            content_type: Optional content type to publish.
            cache_control_max_age: Optional cache max-age to publish.

        Returns:
            Runtime-neutral writer state backed by a session staging file.
        """
        normalized = self.normalize_path(pathname)
        if normalized.endswith("/"):
            raise ValueError("Blob writers cannot open a trailing slash pathname")
        if access not in ("public", "private"):
            raise ValueError('access must be "public" or "private"')
        if not mode.writing:
            raise ValueError("writer mode must be writable")
        self._ensure_open()

        existing: BlobStatResult | None = None
        if mode.appending or (mode.updating and not mode.truncating and not mode.exclusive):
            try:
                existing = await self._api_client.stat(normalized)
            except BlobNotFoundError:
                if not mode.appending:
                    raise

        return await create_writer_state(
            pathname=normalized,
            mode_name=mode.value,
            mode=mode,
            context=self._staging_file_runtime.temporary_file(),
            api_client=self._api_client,
            access=access,
            content_type=content_type,
            cache_control_max_age=cache_control_max_age,
            multipart_threshold=self._options.multipart_threshold,
            multipart_part_size=self._options.multipart_part_size,
            ensure_open=self._ensure_open,
            existing=existing,
        )

    async def scandir_page(
        self,
        *,
        prefix: StrPath,
        mode: ScandirMode,
        page_size: int | None,
        cursor: str | None,
    ) -> BlobPageState:
        """Fetch one listing page for a normalized or caller-provided prefix.

        Args:
            prefix: Store-relative prefix to list.
            mode: Listing mode.
            page_size: Optional backend page size hint.
            cursor: Optional continuation cursor.

        Returns:
            One page of object and prefix states.
        """
        normalized = self.normalize_path(prefix, allow_empty=True)
        return await self._scandir_page_normalized(
            prefix=normalized,
            mode=mode,
            page_size=page_size,
            cursor=cursor,
        )

    async def _scandir_page_normalized(
        self,
        *,
        prefix: str,
        mode: ScandirMode,
        page_size: int | None,
        cursor: str | None,
    ) -> BlobPageState:
        self._ensure_open()
        return await self._api_client.list_page(
            prefix=prefix,
            mode=mode,
            page_size=page_size,
            cursor=cursor,
        )

    async def scandir(
        self,
        *,
        prefix: StrPath = "",
        mode: ScandirMode = ScandirMode.FOLDED,
        page_size: int | None = None,
        cursor: str | None = None,
    ) -> AsyncIterator[BlobEntryState]:
        """Iterate listing pages until the Blob API has no continuation cursor.

        Args:
            prefix: Store-relative prefix to list.
            mode: Listing mode.
            page_size: Optional backend page size hint.
            cursor: Optional continuation cursor.

        Yields:
            Object and prefix state entries.
        """
        normalized = self.normalize_path(prefix, allow_empty=True)
        next_cursor = cursor
        seen_cursors = {cursor} if cursor else set()
        while True:
            page = await self._scandir_page_normalized(
                prefix=normalized,
                mode=mode,
                page_size=page_size,
                cursor=next_cursor,
            )
            next_cursor = _continuation_cursor(page, seen_cursors)
            for entry in page.entries:
                yield entry
            if next_cursor is None:
                return

    async def remove(
        self,
        pathname: StrPath,
        *,
        missing_ok: bool,
        if_match: str | None = None,
    ) -> None:
        """Delete one exact object pathname.

        Args:
            pathname: Store-relative object pathname.
            missing_ok: Whether a missing object should be ignored.
            if_match: Optional ETag precondition.
        """
        normalized = self.normalize_path(pathname)
        self._ensure_open()
        try:
            await self._api_client.delete(normalized, if_match=if_match)
        except BlobNotFoundError:
            if not missing_ok:
                raise

    async def rmtree(self, pathname: StrPath, *, missing_ok: bool) -> None:
        """Delete every object below a prefix.

        Args:
            pathname: Store-relative prefix pathname.
            missing_ok: Whether an empty or missing prefix should be ignored.
        """
        normalized = self._normalize_prefix(pathname)
        attempted = 0
        successful = 0
        failures: list[BaseException] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()

        while True:
            try:
                page = await self._scandir_page_normalized(
                    prefix=normalized,
                    mode=ScandirMode.EXPANDED,
                    page_size=_DELETE_BATCH_SIZE,
                    cursor=cursor,
                )
                next_cursor = _continuation_cursor(page, seen_cursors)
            except Exception as exc:
                failures.append(exc)
                break

            pathnames = [entry.pathname for entry in page.entries]
            attempted += len(pathnames)
            for start in range(0, len(pathnames), _DELETE_BATCH_SIZE):
                batch = pathnames[start : start + _DELETE_BATCH_SIZE]
                self._ensure_open()
                try:
                    await self._api_client.delete_batch(batch)
                except Exception as exc:
                    failures.append(exc)
                else:
                    successful += len(batch)

            if next_cursor is None:
                break
            cursor = next_cursor

        if failures:
            raise BlobRecursiveDeleteError(
                normalized,
                attempted=attempted,
                successful=successful,
                failures=failures,
            )
        if attempted == 0 and not missing_ok:
            raise FileNotFoundError(normalized)

    async def mkdir(
        self,
        pathname: StrPath,
        *,
        access: Access,
        exist_ok: bool,
    ) -> None:
        """Create a zero-byte marker object for a prefix.

        Args:
            pathname: Store-relative prefix pathname.
            access: Delivery access to publish with the marker.
            exist_ok: Whether an existing marker should be accepted.
        """
        normalized = self._normalize_prefix(pathname)
        self._ensure_open()
        await self._api_client.create_marker(normalized, access=access, exist_ok=exist_ok)

    @overload
    async def presign(
        self,
        pathname: StrPath,
        *,
        operation: Literal[PresignedOperation.GET, PresignedOperation.HEAD],
        access: Access,
        expires_in: timedelta,
        maximum_size: None = None,
        allowed_content_types: None = None,
        allow_overwrite: None = None,
        cache_control_max_age: None = None,
        if_match: None = None,
    ) -> PresignedUrl: ...

    @overload
    async def presign(
        self,
        pathname: StrPath,
        *,
        operation: Literal[PresignedOperation.DELETE],
        access: Access,
        expires_in: timedelta,
        maximum_size: None = None,
        allowed_content_types: None = None,
        allow_overwrite: None = None,
        cache_control_max_age: None = None,
        if_match: str | None = None,
    ) -> PresignedUrl: ...

    @overload
    async def presign(
        self,
        pathname: StrPath,
        *,
        operation: Literal[PresignedOperation.PUT],
        access: Access,
        expires_in: timedelta,
        maximum_size: int | None = None,
        allowed_content_types: Sequence[str] | None = None,
        allow_overwrite: bool | None = None,
        cache_control_max_age: timedelta | None = None,
        if_match: str | None = None,
    ) -> PresignedUrl: ...

    @overload
    async def presign(
        self,
        pathname: StrPath,
        *,
        operation: PresignedOperation | str,
        access: Access,
        expires_in: timedelta,
        maximum_size: int | None = None,
        allowed_content_types: Sequence[str] | None = None,
        allow_overwrite: bool | None = None,
        cache_control_max_age: timedelta | None = None,
        if_match: str | None = None,
    ) -> PresignedUrl: ...

    async def presign(
        self,
        pathname: StrPath,
        *,
        operation: PresignedOperation | str,
        access: Access,
        expires_in: timedelta,
        maximum_size: int | None = None,
        allowed_content_types: Sequence[str] | None = None,
        allow_overwrite: bool | None = None,
        cache_control_max_age: timedelta | None = None,
        if_match: str | None = None,
    ) -> PresignedUrl:
        """Create a presigned URL for a Blob operation.

        Args:
            pathname: Store-relative object pathname.
            operation: Operation the URL may perform.
            access: Delivery access for read URLs.
            expires_in: Requested URL lifetime.
            maximum_size: Optional maximum upload size for PUT URLs.
            allowed_content_types: Optional content type allow-list for PUT
                URLs.
            allow_overwrite: Optional overwrite policy for PUT URLs.
            cache_control_max_age: Optional cache max-age for PUT URLs.
            if_match: Optional ETag precondition for PUT or DELETE URLs.

        Returns:
            The presigned URL and its effective expiry.
        """
        normalized = self.normalize_path(pathname)
        if not isinstance(operation, PresignedOperation):
            operation = PresignedOperation(operation)
        if access not in ("public", "private"):
            raise ValueError('access must be "public" or "private"')
        if not isinstance(expires_in, timedelta):
            raise TypeError("expires_in must be a timedelta")
        if expires_in <= timedelta(0):
            raise ValueError("expires_in must be positive")
        if expires_in > _MAX_PRESIGN_AGE:
            raise ValueError("expires_in cannot be more than seven days")
        if operation in (PresignedOperation.GET, PresignedOperation.HEAD) and any(
            value is not None
            for value in (
                maximum_size,
                allowed_content_types,
                allow_overwrite,
                cache_control_max_age,
                if_match,
            )
        ):
            raise ValueError(f"{operation.value} presigning accepts only expiry")
        if operation is PresignedOperation.DELETE and any(
            value is not None
            for value in (
                maximum_size,
                allowed_content_types,
                allow_overwrite,
                cache_control_max_age,
            )
        ):
            raise ValueError("delete presigning accepts only expiry and if_match")
        expires_at = datetime.now(timezone.utc) + expires_in
        self._ensure_open()
        return await self._api_client.presign(
            normalized,
            operation=operation,
            access=access,
            expires_at=expires_at,
            maximum_size=maximum_size,
            allowed_content_types=allowed_content_types,
            allow_overwrite=allow_overwrite,
            cache_control_max_age=cache_control_max_age,
            if_match=if_match,
        )


def get_blob_service(session: "_BaseSdkSession") -> BlobService:
    """Resolve the Blob service attached to an SDK session.

    Args:
        session: Active sync or async SDK session.

    Returns:
        The cached Blob service for the session, creating it on first use.
    """

    def factory() -> BlobService:
        from vercel._internal.unstable.session import SyncSdkSession

        options = session.get_service_option(BlobServiceOptions) or BlobServiceOptions()
        credentials_resolver: _BlobCredentialsResolver
        if isinstance(session, SyncSdkSession):
            if options.sync_credentials_factory is None:
                raise BlobCredentialsError(
                    "Synchronous Blob sessions require sync_credentials_factory when "
                    "custom async credentials_factory is configured"
                )
            credentials_resolver = _SyncBlobCredentialsResolver(options.sync_credentials_factory)
        else:
            credentials_resolver = _AsyncBlobCredentialsResolver(options.credentials_factory)
        return BlobService(
            api_client=BlobApiClient(
                base_url=options.base_url,
                transport=session.get_transport(),
                credentials_resolver=credentials_resolver,
            ),
            options=options,
            ensure_open=session.check_open,
            staging_file_runtime=session.get_staging_file_runtime(),
        )

    return session.get_or_create_service(BlobService, factory)
