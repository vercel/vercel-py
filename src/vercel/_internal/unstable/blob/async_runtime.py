"""Asynchronous file-like facades for Blob streams."""

import codecs
import io
from collections.abc import Awaitable, Callable, Generator
from datetime import timedelta
from types import TracebackType
from typing import Any, Generic, Protocol, TypeVar, cast

import anyio
from typing_extensions import Buffer

from vercel._internal.blob.types import Access
from vercel._internal.unstable.blob.models import BlobStatResult, _FileMode
from vercel._internal.unstable.blob.reader import BlobReaderState, BlobTextReaderState
from vercel._internal.unstable.blob.service import BlobService, StrPath
from vercel._internal.unstable.blob.writer import BlobTextWriterState, BlobWriterState

_T = TypeVar("_T")


class _AsyncContextStream(Protocol):
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...


_StreamT = TypeVar("_StreamT", bound=_AsyncContextStream)


class OpenBlobOperation(Generic[_StreamT]):
    """Single-use deferred Blob stream open operation."""

    def __init__(self, opener: Callable[[], Awaitable[_StreamT]]) -> None:
        self._opener = opener
        self._consumed = False
        self._stream: _StreamT | None = None

    async def _run_once(self) -> _StreamT:
        if self._consumed:
            raise RuntimeError("blob.open(...) operations can only be used once")
        self._consumed = True
        return await self._opener()

    def __await__(self) -> Generator[Any, None, _StreamT]:
        return self._run_once().__await__()

    async def __aenter__(self) -> _StreamT:
        stream = await self._run_once()
        self._stream = stream
        return stream

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        stream = self._stream
        if stream is None:
            return None
        await stream.__aexit__(exc_type, exc, traceback)
        return None


def validate_text_options(
    *,
    encoding: str | None,
    errors: str | None,
    newline: str | None,
) -> tuple[str, str]:
    """Validate text configuration without acquiring a Blob resource."""
    normalized_encoding = "utf-8" if encoding is None else encoding
    normalized_errors = "strict" if errors is None else errors
    if newline not in (None, "", "\n", "\r", "\r\n"):
        raise ValueError(f"illegal newline value: {newline!r}")
    codecs.lookup(normalized_encoding)
    codecs.lookup_error(normalized_errors)
    codecs.getincrementalencoder(normalized_encoding)(errors=normalized_errors)
    codecs.getincrementaldecoder(normalized_encoding)(errors=normalized_errors)
    return normalized_encoding, normalized_errors


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
) -> "AsyncBlobBinaryStream | AsyncBlobTextStream":
    if mode.writing:
        state = await service.open_writer(
            pathname,
            mode=mode,
            access=access,
            content_type=content_type,
            cache_control_max_age=cache_control_max_age,
        )
        binary: AsyncBlobBinaryStream = AsyncBlobBinaryWriter(state)
        if mode.binary:
            return binary
        try:
            return AsyncBlobTextWriter(
                cast(AsyncBlobBinaryWriter, binary),
                encoding="utf-8" if encoding is None else encoding,
                errors="strict" if errors is None else errors,
                newline=newline,
            )
        except BaseException as primary:
            try:
                with anyio.CancelScope(shield=True):
                    await state.abort()
            except BaseException as cleanup:
                raise primary from cleanup
            raise

    reader_state = await service.open_reader(pathname, access=access)
    binary = AsyncBlobBinaryStream(reader_state)
    if mode.binary:
        return binary
    return AsyncBlobTextStream(
        binary,
        encoding="utf-8" if encoding is None else encoding,
        errors="strict" if errors is None else errors,
        newline=newline,
    )


class AsyncBlobBinaryStream:
    """Asynchronous binary reader for an ETag-pinned Blob object."""

    def __init__(self, state: BlobReaderState) -> None:
        self._state = state
        self._lock = anyio.Lock()

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
        return not self.closed

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return not self.closed

    def tell(self) -> int:
        return self._state.tell()

    async def read(self, size: int = -1) -> bytes:
        """Read bytes from the current Blob cursor.

        Args:
            size: Maximum number of bytes to read, or ``-1`` for all remaining
                bytes.

        Returns:
            Bytes read from the object.
        """
        async with self._lock:
            return await self._state.read(size)

    async def write(self, data: Buffer) -> int:
        raise io.UnsupportedOperation("not writable")

    async def flush(self) -> None:
        raise io.UnsupportedOperation("not writable")

    async def truncate(self, size: int | None = None) -> int:
        raise io.UnsupportedOperation("not writable")

    async def readinto(self, buffer: Any) -> int:
        async with self._lock:
            return await self._state.readinto(buffer)

    async def readline(self, size: int = -1) -> bytes:
        async with self._lock:
            return await self._state.readline(size)

    async def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        async with self._lock:
            return await self._state.seek(offset, whence)

    async def close(self) -> None:
        """Close the stream and any active range response."""
        await self._state.close()
        async with self._lock:
            pass

    def __aiter__(self) -> "AsyncBlobBinaryStream":
        return self

    async def __anext__(self) -> bytes:
        line = await self.readline()
        if not line:
            raise StopAsyncIteration
        return line

    async def __aenter__(self) -> "AsyncBlobBinaryStream":
        self._state._check_io()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()


class AsyncBlobTextStream:
    """Asynchronous text reader layered over a binary Blob reader."""

    def __init__(
        self,
        binary: AsyncBlobBinaryStream,
        *,
        encoding: str = "utf-8",
        errors: str = "strict",
        newline: str | None = None,
    ) -> None:
        self._binary = binary
        self._lock = anyio.Lock()
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
    def encoding(self) -> str:
        """Text encoding used by the stream."""
        return self._state.encoding

    @property
    def errors(self) -> str:
        """Text codec error handler used by the stream."""
        return self._state.errors

    @property
    def newlines(self) -> str | tuple[str, ...] | None:
        """Newline forms observed by the text decoder."""
        return self._state.newlines

    def readable(self) -> bool:
        return not self.closed

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return not self.closed

    def tell(self) -> int:
        return self._state.tell()

    async def read(self, size: int = -1) -> str:
        """Read text from the current Blob cursor.

        Args:
            size: Maximum number of characters to read, or ``-1`` for all
                remaining text.

        Returns:
            Decoded text.
        """
        async with self._lock:
            return await self._state.read(size)

    async def write(self, text: str) -> int:
        raise io.UnsupportedOperation("not writable")

    async def flush(self) -> None:
        raise io.UnsupportedOperation("not writable")

    async def truncate(self, size: int | None = None) -> int:
        raise io.UnsupportedOperation("not writable")

    async def readline(self, size: int = -1) -> str:
        async with self._lock:
            return await self._state.readline(size)

    async def seek(self, cookie: int, whence: int = io.SEEK_SET) -> int:
        async with self._lock:
            return await self._state.seek(cookie, whence)

    async def close(self) -> None:
        """Close the text stream and its underlying binary stream."""
        await self._binary.close()
        self._state._clear_cookie_secret()
        async with self._lock:
            pass

    def __aiter__(self) -> "AsyncBlobTextStream":
        return self

    async def __anext__(self) -> str:
        line = await self.readline()
        if not line:
            raise StopAsyncIteration
        return line

    async def __aenter__(self) -> "AsyncBlobTextStream":
        self._state.binary._check_io()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()


class AsyncBlobBinaryWriter(AsyncBlobBinaryStream):
    """Asynchronous binary writer that publishes on successful close."""

    def __init__(self, state: BlobWriterState) -> None:
        self._writer_state = state
        state.defer_failure_cleanup()
        self._lock = anyio.Lock()

    async def _run(self, operation: Callable[[], Awaitable[_T]]) -> _T:
        async with self._lock:
            try:
                return await operation()
            except BaseException as exc:
                if self._writer_state._broken is None:
                    raise
                retry = (
                    isinstance(exc, anyio.get_cancelled_exc_class())
                    and self._writer_state._context_exit_attempted
                )
                with anyio.CancelScope(shield=True):
                    await self._writer_state._cleanup_after_failure(retry=retry)
                broken = self._writer_state._broken
                if broken is not None and broken is not exc:
                    raise broken from exc
                raise

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
        self._writer_state._check_io()
        return self._writer_state.mode.reading

    def writable(self) -> bool:
        self._writer_state._check_io()
        return True

    def seekable(self) -> bool:
        self._writer_state._check_io()
        return self._writer_state.mode.requires_staging

    def tell(self) -> int:
        return self._writer_state.tell()

    async def write(self, data: Buffer) -> int:
        """Stage bytes for publication.

        Args:
            data: Bytes-like data to write.

        Returns:
            Number of bytes accepted.
        """
        return await self._run(lambda: self._writer_state.write(data))

    async def flush(self) -> None:
        await self._run(self._writer_state.flush)

    async def read(self, size: int | None = -1) -> bytes:
        return await self._run(lambda: self._writer_state.read(size))

    async def readinto(self, buffer: Any) -> int:
        return await self._run(lambda: self._writer_state.readinto(buffer))

    async def readline(self, size: int | None = -1) -> bytes:
        return await self._run(lambda: self._writer_state.readline(size))

    async def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        return await self._run(lambda: self._writer_state.seek(offset, whence))

    async def truncate(self, size: int | None = None) -> int:
        return await self._run(lambda: self._writer_state.truncate(size))

    async def close(self) -> None:
        """Publish staged data and close the staging resources."""
        await self._run(self._writer_state.close)

    async def __aenter__(self) -> "AsyncBlobBinaryWriter":
        self._writer_state._check_io()
        return self

    async def __aexit__(self, *args: object) -> None:
        exc_type = args[0] if args else None
        if exc_type is None:
            await self.close()
        else:
            with anyio.CancelScope(shield=True):
                async with self._lock:
                    await self._writer_state.abort()


class AsyncBlobTextWriter(AsyncBlobTextStream):
    """Asynchronous text writer that publishes on successful close."""

    def __init__(
        self,
        binary: AsyncBlobBinaryWriter,
        *,
        encoding: str = "utf-8",
        errors: str = "strict",
        newline: str | None = None,
    ) -> None:
        self._writer_binary = binary
        self._writer_state = BlobTextWriterState(
            binary._writer_state, encoding=encoding, errors=errors, newline=newline
        )
        self._lock = anyio.Lock()

    async def _run(self, operation: Callable[[], Awaitable[_T]]) -> _T:
        async with self._lock:
            try:
                return await operation()
            except BaseException as exc:
                binary = self._writer_binary._writer_state
                if binary._broken is None:
                    raise
                retry = (
                    isinstance(exc, anyio.get_cancelled_exc_class())
                    and binary._context_exit_attempted
                )
                with anyio.CancelScope(shield=True):
                    await binary._cleanup_after_failure(retry=retry)
                broken = binary._broken
                if broken is not None and broken is not exc:
                    raise broken from exc
                raise

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
    def encoding(self) -> str:
        return self._writer_state.encoding

    @property
    def errors(self) -> str:
        return self._writer_state.errors

    @property
    def newlines(self) -> str | tuple[str, ...] | None:
        return getattr(self._writer_state._decoder, "newlines", None)

    def readable(self) -> bool:
        return self._writer_binary.readable()

    def writable(self) -> bool:
        return self._writer_binary.writable()

    def seekable(self) -> bool:
        return self._writer_binary.seekable()

    def tell(self) -> int:
        return self._writer_state.tell()

    async def write(self, text: str) -> int:
        """Stage text for publication.

        Args:
            text: Text to encode and write.

        Returns:
            Number of input characters accepted.
        """
        return await self._run(lambda: self._writer_state.write(text))

    async def read(self, size: int | None = -1) -> str:
        return await self._run(lambda: self._writer_state.read(size))

    async def readline(self, size: int | None = -1) -> str:
        return await self._run(lambda: self._writer_state.readline(size))

    async def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        return await self._run(lambda: self._writer_state.seek(offset, whence))

    async def truncate(self, size: int | None = None) -> int:
        return await self._run(lambda: self._writer_state.truncate(size))

    async def flush(self) -> None:
        await self._run(self._writer_state.flush)

    async def close(self) -> None:
        """Finalize text encoding, publish staged data, and close resources."""
        await self._run(self._writer_state.close)

    async def __aenter__(self) -> "AsyncBlobTextWriter":
        self._writer_binary._writer_state._check_io()
        return self

    async def __aexit__(self, *args: object) -> None:
        exc_type = args[0] if args else None
        if exc_type is None:
            await self.close()
        else:
            with anyio.CancelScope(shield=True):
                async with self._lock:
                    await self._writer_state.abort()


__all__ = [
    "AsyncBlobBinaryStream",
    "AsyncBlobBinaryWriter",
    "AsyncBlobTextStream",
    "AsyncBlobTextWriter",
    "OpenBlobOperation",
]
