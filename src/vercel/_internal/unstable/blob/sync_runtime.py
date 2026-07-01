"""Synchronous file-like facades for Blob streams."""

import io
import threading
from datetime import timedelta
from typing import Any, cast

from typing_extensions import Buffer

from vercel._internal.blob.types import Access
from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.unstable.blob.models import BlobStatResult, _FileMode
from vercel._internal.unstable.blob.reader import BlobReaderState, BlobTextReaderState
from vercel._internal.unstable.blob.service import BlobService, StrPath
from vercel._internal.unstable.blob.writer import BlobTextWriterState, BlobWriterState


async def open_blob_stream(
    service: BlobService,
    pathname: StrPath,
    *,
    mode: _FileMode,
    access: Access,
    encoding: str | None,
    errors: str | None,
    newline: str | None,
    content_type: str | None,
    cache_control_max_age: timedelta | None,
) -> "SyncBlobBinaryStream | SyncBlobTextStream":
    if mode.writing:
        state = await service.open_writer(
            pathname,
            mode=mode,
            access=access,
            content_type=content_type,
            cache_control_max_age=cache_control_max_age,
        )
        try:
            binary: SyncBlobBinaryStream = SyncBlobBinaryWriter(state)
        except BaseException as primary:
            try:
                await state.abort()
            except BaseException as cleanup:
                raise primary from cleanup
            raise
        if mode.binary:
            return binary
        try:
            return SyncBlobTextWriter(
                cast(SyncBlobBinaryWriter, binary),
                encoding="utf-8" if encoding is None else encoding,
                errors="strict" if errors is None else errors,
                newline=newline,
            )
        except BaseException as primary:
            try:
                await state.abort()
            except BaseException as cleanup:
                raise primary from cleanup
            raise

    reader_state = await service.open_reader(pathname, access=access)
    try:
        binary = SyncBlobBinaryStream(reader_state)
    except BaseException as primary:
        try:
            await reader_state.close()
        except BaseException as cleanup:
            raise primary from cleanup
        raise
    if mode.binary:
        return binary
    try:
        return SyncBlobTextStream(
            binary,
            encoding="utf-8" if encoding is None else encoding,
            errors="strict" if errors is None else errors,
            newline=newline,
        )
    except BaseException as primary:
        try:
            await reader_state.close()
        except BaseException as cleanup:
            raise primary from cleanup
        raise


class SyncBlobBinaryStream(io.BufferedIOBase):
    """Synchronous binary reader for an ETag-pinned Blob object."""

    def __init__(self, state: BlobReaderState) -> None:
        self._state = state
        self._lock = threading.RLock()
        self._close_lock = threading.Lock()

    @property
    def closed(self) -> bool:
        """Whether the stream has been closed."""
        return self._state.closed

    @property
    def name(self) -> str:
        """Store-relative pathname for the stream."""
        return self._state.stat.pathname

    @property
    def mode(self) -> str:
        """Python file mode reported by the stream."""
        return "rb"

    @property
    def stat(self) -> BlobStatResult:
        """Metadata captured when the Blob reader opened."""
        return self._state.stat

    def readable(self) -> bool:
        with self._lock:
            self._state._check_io()
            return True

    def writable(self) -> bool:
        with self._lock:
            self._state._check_io()
            return False

    def seekable(self) -> bool:
        with self._lock:
            self._state._check_io()
            return True

    def tell(self) -> int:
        with self._lock:
            return self._state.tell()

    def read(self, size: int | None = -1) -> bytes:
        """Read bytes from the current Blob cursor.

        Args:
            size: Maximum number of bytes to read, or ``-1`` for all remaining
                bytes.

        Returns:
            Bytes read from the object.
        """
        with self._lock:
            return iter_coroutine(self._state.read(size))

    def read1(self, size: int = -1) -> bytes:
        return self.read(size)

    def readinto(self, buffer: Any) -> int:
        with self._lock:
            return iter_coroutine(self._state.readinto(buffer))

    def readinto1(self, buffer: Any) -> int:
        return self.readinto(buffer)

    def readline(self, size: int | None = -1) -> bytes:
        with self._lock:
            return iter_coroutine(self._state.readline(size))

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        with self._lock:
            return iter_coroutine(self._state.seek(offset, whence))

    def close(self) -> None:
        with self._close_lock:
            if not self.closed:
                iter_coroutine(self._state.close())
            with self._lock:
                super().close()


class SyncBlobTextStream(io.TextIOBase):
    """Synchronous text reader layered over a binary Blob reader."""

    def __init__(
        self,
        binary: SyncBlobBinaryStream,
        *,
        encoding: str = "utf-8",
        errors: str = "strict",
        newline: str | None = None,
    ) -> None:
        self._binary = binary
        self._lock = threading.RLock()
        self._close_lock = threading.Lock()
        self._state = BlobTextReaderState(
            binary._state,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )

    @property
    def closed(self) -> bool:
        """Whether the stream has been closed."""
        return self._state.closed

    @property
    def name(self) -> str:
        """Store-relative pathname for the stream."""
        return self._binary.name

    @property
    def mode(self) -> str:
        """Python file mode reported by the stream."""
        return "r"

    @property
    def stat(self) -> BlobStatResult:
        """Metadata captured when the Blob reader opened."""
        return self._binary.stat

    @property
    def encoding(self) -> str:  # type: ignore[override]
        """Text encoding used by the stream."""
        return self._state.encoding

    @property
    def errors(self) -> str:  # type: ignore[override]
        """Text codec error handler used by the stream."""
        return self._state.errors

    @property
    def newlines(self) -> str | tuple[str, ...] | None:  # type: ignore[override]
        """Newline forms observed by the text decoder."""
        return self._state.newlines

    def readable(self) -> bool:
        with self._lock:
            self._state.binary._check_io()
            return True

    def writable(self) -> bool:
        with self._lock:
            self._state.binary._check_io()
            return False

    def seekable(self) -> bool:
        with self._lock:
            self._state.binary._check_io()
            return True

    def tell(self) -> int:
        with self._lock:
            return self._state.tell()

    def read(self, size: int | None = -1) -> str:
        """Read text from the current Blob cursor.

        Args:
            size: Maximum number of characters to read, or ``-1`` for all
                remaining text.

        Returns:
            Decoded text.
        """
        with self._lock:
            return iter_coroutine(self._state.read(size))

    def readline(self, size: int | None = -1) -> str:  # type: ignore[override]
        with self._lock:
            return iter_coroutine(self._state.readline(size))

    def seek(self, cookie: int, whence: int = io.SEEK_SET) -> int:
        with self._lock:
            return iter_coroutine(self._state.seek(cookie, whence))

    def close(self) -> None:
        with self._close_lock:
            self._binary.close()
            self._state._clear_cookie_secret()
            with self._lock:
                super().close()


class SyncBlobBinaryWriter(SyncBlobBinaryStream):
    """Synchronous binary writer that publishes on successful close."""

    def __init__(self, state: BlobWriterState) -> None:
        self._writer_state = state
        self._lock = threading.RLock()
        self._close_lock = threading.Lock()
        self._base_closing = False

    @property
    def closed(self) -> bool:
        return self._writer_state.closed

    @property
    def name(self) -> str:
        return self._writer_state.pathname

    @property
    def mode(self) -> str:
        return self._writer_state.mode_name

    @property
    def stat(self) -> BlobStatResult:
        """Metadata for the published object.

        Raises:
            ValueError: If the writer has not published successfully.
        """
        return self._writer_state.stat

    def readable(self) -> bool:
        with self._lock:
            self._writer_state._check_io()
            return self._writer_state.mode.reading

    def writable(self) -> bool:
        with self._lock:
            self._writer_state._check_io()
            return True

    def seekable(self) -> bool:
        with self._lock:
            self._writer_state._check_io()
            return self._writer_state.mode.requires_staging

    def tell(self) -> int:
        with self._lock:
            return self._writer_state.tell()

    def write(self, data: Buffer) -> int:
        """Stage bytes for publication.

        Args:
            data: Bytes-like data to write.

        Returns:
            Number of bytes accepted.
        """
        with self._lock:
            return iter_coroutine(self._writer_state.write(data))

    def flush(self) -> None:
        with self._lock:
            if self._base_closing:
                return
            self._writer_state._check_io()
            iter_coroutine(self._writer_state.flush())

    def read(self, size: int | None = -1) -> bytes:
        with self._lock:
            return iter_coroutine(self._writer_state.read(size))

    def read1(self, size: int = -1) -> bytes:
        return self.read(size)

    def readinto(self, buffer: Any) -> int:
        with self._lock:
            return iter_coroutine(self._writer_state.readinto(buffer))

    def readinto1(self, buffer: Any) -> int:
        return self.readinto(buffer)

    def readline(self, size: int | None = -1) -> bytes:
        with self._lock:
            return iter_coroutine(self._writer_state.readline(size))

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        with self._lock:
            return iter_coroutine(self._writer_state.seek(offset, whence))

    def truncate(self, size: int | None = None) -> int:
        with self._lock:
            return iter_coroutine(self._writer_state.truncate(size))

    def close(self) -> None:
        """Publish staged data and close the staging resources."""
        with self._close_lock:
            with self._lock:
                iter_coroutine(self._writer_state.close())
                self._base_closing = True
                try:
                    io.BufferedIOBase.close(self)
                finally:
                    self._base_closing = False

    def __enter__(self) -> "SyncBlobBinaryWriter":
        self._writer_state._check_io()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> None:
        if exc_type is None:
            self.close()
        else:
            with self._lock:
                iter_coroutine(self._writer_state.abort())


class SyncBlobTextWriter(SyncBlobTextStream):
    """Synchronous text writer that publishes on successful close."""

    def __init__(
        self,
        binary: SyncBlobBinaryWriter,
        *,
        encoding: str = "utf-8",
        errors: str = "strict",
        newline: str | None = None,
    ) -> None:
        self._writer_binary = binary
        self._writer_state = BlobTextWriterState(
            binary._writer_state, encoding=encoding, errors=errors, newline=newline
        )
        self._lock = threading.RLock()
        self._close_lock = threading.Lock()
        self._base_closing = False

    @property
    def closed(self) -> bool:
        return self._writer_binary.closed

    @property
    def name(self) -> str:
        return self._writer_binary.name

    @property
    def mode(self) -> str:
        return self._writer_binary.mode.replace("b", "")

    @property
    def stat(self) -> BlobStatResult:
        """Metadata for the published object.

        Raises:
            ValueError: If the writer has not published successfully.
        """
        return self._writer_binary.stat

    @property
    def encoding(self) -> str:  # type: ignore[override]
        return self._writer_state.encoding

    @property
    def errors(self) -> str:  # type: ignore[override]
        return self._writer_state.errors

    @property
    def newlines(self) -> str | tuple[str, ...] | None:  # type: ignore[override]
        return getattr(self._writer_state._decoder, "newlines", None)

    def readable(self) -> bool:
        return self._writer_binary.readable()

    def writable(self) -> bool:
        return self._writer_binary.writable()

    def seekable(self) -> bool:
        return self._writer_binary.seekable()

    def tell(self) -> int:
        with self._lock:
            return self._writer_state.tell()

    def write(self, text: str) -> int:
        """Stage text for publication.

        Args:
            text: Text to encode and write.

        Returns:
            Number of input characters accepted.
        """
        with self._lock:
            return iter_coroutine(self._writer_state.write(text))

    def read(self, size: int | None = -1) -> str:
        with self._lock:
            return iter_coroutine(self._writer_state.read(size))

    def readline(self, size: int | None = -1) -> str:  # type: ignore[override]
        with self._lock:
            return iter_coroutine(self._writer_state.readline(size))

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        with self._lock:
            return iter_coroutine(self._writer_state.seek(offset, whence))

    def truncate(self, size: int | None = None) -> int:
        with self._lock:
            return iter_coroutine(self._writer_state.truncate(size))

    def flush(self) -> None:
        with self._lock:
            if self._base_closing:
                return
            self._writer_binary._writer_state._check_io()
            iter_coroutine(self._writer_state.flush())

    def close(self) -> None:
        """Finalize text encoding, publish staged data, and close resources."""
        with self._close_lock:
            with self._lock:
                iter_coroutine(self._writer_state.close())
                self._base_closing = True
                try:
                    io.TextIOBase.close(self)
                finally:
                    self._base_closing = False

    def __enter__(self) -> "SyncBlobTextWriter":
        self._writer_binary._writer_state._check_io()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object,
    ) -> None:
        if exc_type is None:
            self.close()
        else:
            with self._lock:
                iter_coroutine(self._writer_state.abort())


__all__ = [
    "open_blob_stream",
    "SyncBlobBinaryStream",
    "SyncBlobBinaryWriter",
    "SyncBlobTextStream",
    "SyncBlobTextWriter",
]
