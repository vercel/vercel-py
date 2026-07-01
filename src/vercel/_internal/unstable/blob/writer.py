"""Runtime-neutral transactional Blob writer engines."""

import base64
import codecs
import hashlib
import hmac
import io
import json
import os
import secrets
import threading
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from datetime import timedelta
from typing import Any

from typing_extensions import Buffer

from vercel._internal.blob.types import Access
from vercel._internal.byte_stream import ReadableByteStream, StagingByteFile
from vercel._internal.unstable.blob.api_client import BlobApiClient
from vercel._internal.unstable.blob.models import (
    BlobStatResult,
    MultipartPartState,
    MultipartUploadState,
    _FileMode,
)

_COPY_CHUNK_SIZE = 64 * 1024
_READ_CHUNK_SIZE = 64 * 1024
_TEXT_COOKIE_PREFIX = b"VBW2:"
_TEXT_COOKIE_SIGNATURE_SIZE = hashlib.sha256().digest_size


class _LimitedStream:
    """A non-owning view that cannot consume bytes beyond one part."""

    def __init__(self, source: ReadableByteStream, size: int) -> None:
        self._source = source
        self._remaining = size

    async def read(self, size: int = -1, /) -> bytes:
        if self._remaining == 0:
            return b""
        if size < 0 or size > self._remaining:
            size = self._remaining
        data = await self._source.read(size)
        self._remaining -= len(data)
        return data


class _Publisher:
    def __init__(
        self,
        *,
        api_client: BlobApiClient,
        pathname: str,
        access: Access,
        content_type: str | None,
        cache_control_max_age: timedelta | None,
        multipart_threshold: int,
        multipart_part_size: int,
        exclusive: bool,
        if_match: str | None,
        ensure_open: Callable[[], None],
    ) -> None:
        self.api_client = api_client
        self.pathname = pathname
        self.access = access
        self.content_type = content_type
        self.cache_control_max_age = cache_control_max_age
        self.multipart_threshold = multipart_threshold
        self.multipart_part_size = multipart_part_size
        self.exclusive = exclusive
        self.if_match = if_match
        self.ensure_open = ensure_open

    async def regular(self, source: StagingByteFile, size: int) -> BlobStatResult:
        self.ensure_open()
        await source.seek(0)
        return await self.api_client.put(
            self.pathname,
            source,
            size=size,
            access=self.access,
            content_type=self.content_type,
            cache_control_max_age=self.cache_control_max_age,
            exclusive=self.exclusive,
            if_match=self.if_match,
        )

    async def create_multipart(self) -> MultipartUploadState:
        self.ensure_open()
        return await self.api_client.create_multipart_upload(
            self.pathname,
            access=self.access,
            content_type=self.content_type,
            cache_control_max_age=self.cache_control_max_age,
        )

    async def part(
        self,
        upload: MultipartUploadState,
        source: StagingByteFile,
        part_number: int,
        size: int,
    ) -> MultipartPartState:
        self.ensure_open()
        return await self.api_client.upload_part(
            upload,
            part_number=part_number,
            source=_LimitedStream(source, size),
            size=size,
        )

    async def complete(
        self, upload: MultipartUploadState, parts: list[MultipartPartState]
    ) -> BlobStatResult:
        self.ensure_open()
        return await self.api_client.complete_multipart_upload(
            upload,
            parts,
            exclusive=self.exclusive,
            if_match=self.if_match,
        )

    async def exact(self, source: StagingByteFile, size: int) -> BlobStatResult:
        if size < self.multipart_threshold:
            return await self.regular(source, size)
        upload = await self.create_multipart()
        parts: list[MultipartPartState] = []
        await source.seek(0)
        remaining = size
        while remaining:
            part_size = min(remaining, self.multipart_part_size)
            parts.append(await self.part(upload, source, len(parts) + 1, part_size))
            remaining -= part_size
        return await self.complete(upload, parts)


async def _write_all(file: StagingByteFile, data: Buffer) -> None:
    view = memoryview(data)
    try:
        source = view.cast("B")
        offset = 0
        while offset < len(source):
            end = min(len(source), offset + _COPY_CHUNK_SIZE)
            written = await file.write(source[offset:end])  # type: ignore[arg-type]
            if written <= 0:
                raise OSError("staging file write made no progress")
            offset += written
    finally:
        view.release()


def _buffer_size(data: Buffer) -> int:
    view = memoryview(data)
    try:
        return view.nbytes
    finally:
        view.release()


class _SequentialEngine:
    """Write engine for non-seekable replacement writes."""

    def __init__(self, file: StagingByteFile, publisher: _Publisher) -> None:
        self.file = file
        self.publisher = publisher
        self.position = 0
        self.staged_size = 0
        self.upload: MultipartUploadState | None = None
        self.parts: list[MultipartPartState] = []

    async def write(self, data: Buffer) -> int:
        size = _buffer_size(data)
        view = memoryview(data)
        source: memoryview | None = None
        try:
            source = view.cast("B")
            offset = 0
            while offset < size:
                if self.upload is None:
                    boundary = self.publisher.multipart_threshold - self.position
                else:
                    boundary = self.publisher.multipart_part_size - self.staged_size
                take = min(size - offset, _COPY_CHUNK_SIZE, max(1, boundary))
                await self.file.seek(self.staged_size)
                await _write_all(self.file, source[offset : offset + take])
                offset += take
                self.position += take
                self.staged_size += take
                if self.upload is None and self.position >= self.publisher.multipart_threshold:
                    self.upload = await self.publisher.create_multipart()
                if self.upload is not None:
                    await self._drain_complete_parts()
        finally:
            if source is not None:
                source.release()
            view.release()
        return size

    async def _drain_complete_parts(self) -> None:
        assert self.upload is not None
        part_size = self.publisher.multipart_part_size
        complete = self.staged_size // part_size
        if complete == 0:
            return
        await self.file.seek(0)
        for _ in range(complete):
            self.parts.append(
                await self.publisher.part(self.upload, self.file, len(self.parts) + 1, part_size)
            )
        consumed = complete * part_size
        trailing = self.staged_size - consumed
        read_at = consumed
        write_at = 0
        while write_at < trailing:
            await self.file.seek(read_at)
            chunk = await self.file.read(min(_COPY_CHUNK_SIZE, trailing - write_at))
            if not chunk:
                raise OSError("staging file ended during compaction")
            read_at += len(chunk)
            await self.file.seek(write_at)
            await _write_all(self.file, chunk)
            write_at += len(chunk)
        await self.file.truncate(trailing)
        await self.file.seek(trailing)
        self.staged_size = trailing

    async def flush(self) -> None:
        await self.file.flush()
        if self.upload is not None:
            await self._drain_complete_parts()

    async def publish(self) -> BlobStatResult:
        if self.upload is None:
            return await self.publisher.regular(self.file, self.staged_size)
        if self.staged_size:
            await self.file.seek(0)
            self.parts.append(
                await self.publisher.part(
                    self.upload, self.file, len(self.parts) + 1, self.staged_size
                )
            )
            self.staged_size = 0
        return await self.publisher.complete(self.upload, self.parts)


class _MutationEngine:
    """Write engine for staged append and update modes."""

    def __init__(
        self,
        file: StagingByteFile,
        publisher: _Publisher,
        mode: _FileMode,
        *,
        size: int,
        dirty: bool,
    ) -> None:
        self.file = file
        self.publisher = publisher
        self.mode = mode
        self.size = size
        self.position = size if mode.appending else 0
        self.dirty = dirty
        self._read_buffer = b""
        self._read_buffer_start = 0

    def _invalidate_read_buffer(self) -> None:
        self._read_buffer = b""
        self._read_buffer_start = self.position

    async def write(self, data: Buffer) -> int:
        size = _buffer_size(data)
        if self.mode.appending:
            self.position = self.size
        if size == 0:
            return 0
        self._invalidate_read_buffer()
        await self.file.seek(self.position)
        await _write_all(self.file, data)
        self.position += size
        self.size = max(self.size, self.position)
        self.dirty = True
        return size

    async def read(self, size: int | None = -1) -> bytes:
        if not self.mode.reading:
            raise io.UnsupportedOperation("not readable")
        self._invalidate_read_buffer()
        await self.file.seek(self.position)
        data = await self.file.read(-1 if size is None else size)
        self.position += len(data)
        return data

    async def readinto(self, buffer: Any) -> int:
        if not self.mode.reading:
            raise io.UnsupportedOperation("not readable")
        self._invalidate_read_buffer()
        await self.file.seek(self.position)
        count = await self.file.readinto(buffer)
        self.position += count
        return count

    async def readline(self, size: int | None = -1) -> bytes:
        if not self.mode.reading:
            raise io.UnsupportedOperation("not readable")
        limit = -1 if size is None else size
        if limit == 0:
            return b""
        result = bytearray()
        while limit < 0 or len(result) < limit:
            buffer_offset = self.position - self._read_buffer_start
            if buffer_offset < 0 or buffer_offset >= len(self._read_buffer):
                await self.file.seek(self.position)
                request_size = _READ_CHUNK_SIZE
                if limit >= 0:
                    request_size = min(request_size, limit - len(result))
                self._read_buffer_start = self.position
                self._read_buffer = await self.file.read(request_size)
                buffer_offset = 0
                if not self._read_buffer:
                    break
            available = self._read_buffer[buffer_offset:]
            if limit >= 0:
                available = available[: limit - len(result)]
            newline = available.find(b"\n")
            take = len(available) if newline < 0 else newline + 1
            result.extend(available[:take])
            self.position += take
            if newline >= 0:
                break
        return bytes(result)

    async def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            position = offset
        elif whence == io.SEEK_CUR:
            position = self.position + offset
        elif whence == io.SEEK_END:
            position = self.size + offset
        else:
            raise ValueError(f"invalid whence ({whence!r})")
        if position < 0:
            raise ValueError("negative seek position")
        self.position = position
        self._invalidate_read_buffer()
        return position

    async def truncate(self, size: int | None = None) -> int:
        target = self.position if size is None else size
        if target < 0:
            raise ValueError("negative truncate size")
        self._invalidate_read_buffer()
        await self.file.truncate(target)
        self.size = target
        self.dirty = True
        return target

    async def flush(self) -> None:
        await self.file.flush()

    async def publish(self) -> BlobStatResult | None:
        if not self.dirty:
            return None
        await self.file.flush()
        return await self.publisher.exact(self.file, self.size)


class BlobWriterState:
    """Shared async-shaped binary writer state owned by one staging context."""

    def __init__(
        self,
        *,
        pathname: str,
        mode_name: str,
        mode: _FileMode,
        context: AbstractAsyncContextManager[StagingByteFile],
        engine: _SequentialEngine | _MutationEngine,
        ensure_open: Callable[[], None],
    ) -> None:
        self.pathname = pathname
        self.mode_name = mode_name
        self.mode = mode
        self._context = context
        self._engine = engine
        self._ensure_open = ensure_open
        self._closed = False
        self._published: BlobStatResult | None = None
        self._broken: BaseException | None = None
        self._context_state = "entered"
        self._context_exit_attempted = False
        self._defer_failure_cleanup = False
        self._lifecycle_lock = threading.RLock()

    @property
    def closed(self) -> bool:
        """Whether the writer state has been closed."""
        with self._lifecycle_lock:
            return self._closed

    @property
    def stat(self) -> BlobStatResult:
        """Metadata for the published object.

        Raises:
            ValueError: If the writer has not published successfully.
        """
        if self._published is None:
            raise ValueError("Blob writer has not published an object")
        return self._published

    def _check_io(self) -> None:
        if self._closed:
            raise ValueError("I/O operation on closed Blob writer")
        if self._broken is not None:
            raise self._broken
        self._ensure_open()

    def tell(self) -> int:
        """Return the current staged byte offset."""
        self._check_io()
        return self._engine.position

    async def _break(self, exc: BaseException) -> None:
        if self._broken is None:
            self._broken = exc
        self._closed = True
        if self._defer_failure_cleanup:
            return
        await self._cleanup_after_failure()

    def defer_failure_cleanup(self) -> None:
        self._defer_failure_cleanup = True

    def _note_cleanup_failure(self, cleanup_error: BaseException) -> None:
        if self._broken is None:
            self._broken = cleanup_error
            return
        add_note = getattr(self._broken, "add_note", None)
        if callable(add_note):
            add_note(f"staging cleanup also failed: {cleanup_error!r}")

    async def _cleanup_after_failure(self, *, retry: bool = False) -> None:
        if self._context_state == "exited":
            return
        if self._context_exit_attempted and not retry:
            return
        try:
            await self._exit_context(self._broken)
        except BaseException as cleanup_error:
            self._note_cleanup_failure(cleanup_error)

    async def write(self, data: Buffer) -> int:
        """Stage bytes for the eventual Blob publication.

        Args:
            data: Bytes-like data to write.

        Returns:
            Number of bytes accepted.
        """
        self._check_io()
        if isinstance(data, str):
            raise TypeError("a bytes-like object is required")
        try:
            return await self._engine.write(data)
        except BaseException as exc:
            await self._break(exc)
            raise

    async def flush(self) -> None:
        """Flush staged bytes without publishing the Blob."""
        self._check_io()
        try:
            await self._engine.flush()
        except BaseException as exc:
            await self._break(exc)
            raise

    async def read(self, size: int | None = -1) -> bytes:
        """Read bytes from the staged file in readable update modes.

        Args:
            size: Maximum number of bytes to read, or ``-1`` for all remaining
                staged bytes.

        Returns:
            Bytes read from the staged file.
        """
        self._check_io()
        if not isinstance(self._engine, _MutationEngine):
            raise io.UnsupportedOperation("not readable")
        try:
            return await self._engine.read(size)
        except OSError as exc:
            await self._break(exc)
            raise

    async def readinto(self, buffer: Any) -> int:
        self._check_io()
        if not isinstance(self._engine, _MutationEngine):
            raise io.UnsupportedOperation("not readable")
        try:
            return await self._engine.readinto(buffer)
        except OSError as exc:
            await self._break(exc)
            raise

    async def readline(self, size: int | None = -1) -> bytes:
        self._check_io()
        if not isinstance(self._engine, _MutationEngine):
            raise io.UnsupportedOperation("not readable")
        try:
            return await self._engine.readline(size)
        except OSError as exc:
            await self._break(exc)
            raise

    async def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        """Move the staged-file cursor in seekable modes.

        Args:
            offset: Byte offset interpreted relative to ``whence``.
            whence: Standard ``io`` seek mode.

        Returns:
            New absolute byte offset.
        """
        self._check_io()
        if not isinstance(self._engine, _MutationEngine):
            raise io.UnsupportedOperation("not seekable")
        return await self._engine.seek(offset, whence)

    async def truncate(self, size: int | None = None) -> int:
        """Resize the staged file in seekable modes.

        Args:
            size: New size, or the current cursor position when omitted.

        Returns:
            New staged file size.
        """
        self._check_io()
        if not isinstance(self._engine, _MutationEngine):
            raise io.UnsupportedOperation("not seekable")
        try:
            return await self._engine.truncate(size)
        except OSError as exc:
            await self._break(exc)
            raise

    async def _exit_context(self, exc: BaseException | None = None) -> None:
        if self._context_state == "exited":
            return
        if self._context_state == "exiting":
            raise RuntimeError("staging cleanup is already in progress")
        self._context_state = "exiting"
        self._context_exit_attempted = True
        try:
            if exc is None:
                await self._context.__aexit__(None, None, None)
            else:
                await self._context.__aexit__(type(exc), exc, exc.__traceback__)
        except BaseException:
            self._context_state = "entered"
            raise
        else:
            self._context_state = "exited"

    async def close(self) -> None:
        """Publish staged data, close the staging context, and store metadata."""
        if self._closed:
            if self._broken is not None:
                raise self._broken
            return
        if self._broken is not None:
            self._closed = True
            await self._exit_context(self._broken)
            raise self._broken
        try:
            self._ensure_open()
            result = await self._engine.publish()
            if result is not None:
                self._published = result
        except BaseException as exc:
            await self._break(exc)
            broken = self._broken
            assert broken is not None
            if broken is exc:
                raise
            raise broken from exc
        self._closed = True
        try:
            await self._exit_context()
        except BaseException as exc:
            if self._broken is None:
                self._broken = exc
            broken = self._broken
            if broken is exc:
                raise
            raise broken from exc

    async def abort(self) -> None:
        """Close staging resources without publishing a Blob."""
        if self._closed:
            return
        self._closed = True
        try:
            await self._exit_context(RuntimeError("Blob writer aborted"))
        except BaseException as exc:
            if self._broken is None:
                self._broken = exc
            broken = self._broken
            if broken is exc:
                raise
            raise broken from exc


async def create_writer_state(
    *,
    pathname: str,
    mode_name: str,
    mode: _FileMode,
    context: AbstractAsyncContextManager[StagingByteFile],
    api_client: BlobApiClient,
    access: Access,
    content_type: str | None,
    cache_control_max_age: timedelta | None,
    multipart_threshold: int,
    multipart_part_size: int,
    ensure_open: Callable[[], None],
    existing: BlobStatResult | None,
) -> BlobWriterState:
    """Create a writer state and materialize existing content when needed.

    Args:
        pathname: Store-relative object pathname.
        mode_name: Original Python file mode string.
        mode: Parsed mode flags.
        context: Async context manager that yields the staging file.
        api_client: Blob wire client.
        access: Delivery access to publish.
        content_type: Optional content type to publish.
        cache_control_max_age: Optional cache max-age to publish.
        multipart_threshold: Size at which sequential writes switch to
            multipart upload.
        multipart_part_size: Multipart part size.
        ensure_open: Session-open guard.
        existing: Existing object metadata for append/update modes.

    Returns:
        Writer state backed by the entered staging context.
    """
    file = await context.__aenter__()
    try:
        if existing is not None:
            position = 0
            while position < existing.size:
                end = min(existing.size, position + _COPY_CHUNK_SIZE) - 1
                ensure_open()
                response = await api_client.read_range(
                    existing, access=access, start=position, end=end
                )
                try:
                    async for chunk in response:
                        await _write_all(file, chunk)
                        position += len(chunk)
                finally:
                    await response.aclose()
                if position != end + 1:
                    raise OSError("Blob materialization ended before the requested range")
            await file.seek(0)

        publisher = _Publisher(
            api_client=api_client,
            pathname=pathname,
            access=access,
            content_type=content_type,
            cache_control_max_age=cache_control_max_age,
            multipart_threshold=multipart_threshold,
            multipart_part_size=multipart_part_size,
            exclusive=mode.exclusive or (mode.appending and existing is None),
            if_match=existing.etag if existing is not None else None,
            ensure_open=ensure_open,
        )
        if mode.requires_staging:
            engine: _SequentialEngine | _MutationEngine = _MutationEngine(
                file,
                publisher,
                mode,
                size=existing.size if existing is not None else 0,
                dirty=mode.truncating or mode.exclusive or (mode.appending and existing is None),
            )
        else:
            engine = _SequentialEngine(file, publisher)
        return BlobWriterState(
            pathname=pathname,
            mode_name=mode_name,
            mode=mode,
            context=context,
            engine=engine,
            ensure_open=ensure_open,
        )
    except BaseException as exc:
        await context.__aexit__(type(exc), exc, exc.__traceback__)
        raise


class BlobTextWriterState:
    """Incremental text encoder layered over a binary Blob writer."""

    def __init__(
        self,
        binary: BlobWriterState,
        *,
        encoding: str,
        errors: str,
        newline: str | None,
    ) -> None:
        if newline not in (None, "", "\n", "\r", "\r\n"):
            raise ValueError(f"illegal newline value: {newline!r}")
        self.binary = binary
        self.encoding = encoding
        self.errors = errors
        self.newline = newline
        self._encoder = codecs.getincrementalencoder(encoding)(errors=errors)
        self._decoder = self._new_decoder()
        self._read_text = ""
        self._read_eof = False
        started = codecs.getincrementalencoder(encoding)(errors=errors)
        started.encode("", final=False)
        self._encoder_started_state = started.getstate()
        if binary.tell() > 0:
            self._encoder.setstate(self._encoder_started_state)
        self._encoder_used = False
        self._last_operation: str | None = None
        self._cookie_secret = secrets.token_bytes(32)
        self._max_cookie_bytes = _READ_CHUNK_SIZE * 12 + 1024
        self._finalized = False

    def _new_decoder(self) -> Any:
        decoder = codecs.getincrementaldecoder(self.encoding)(errors=self.errors)
        if self.newline in (None, ""):
            return io.IncrementalNewlineDecoder(decoder, translate=self.newline is None)
        return decoder

    def _reset_reader(self) -> None:
        self._decoder = self._new_decoder()
        self._read_text = ""
        self._read_eof = False

    def tell(self) -> int:
        """Return an opaque text seek cookie for the current staged position."""
        position = self.binary.tell()
        decoder_state = self._decoder.getstate()
        encoder_state = self._encoder.getstate()
        if isinstance(encoder_state, bool) or not isinstance(encoder_state, int):
            raise io.UnsupportedOperation("encoder does not support seekable state")
        if (
            position == 0
            and not self._read_text
            and not self._read_eof
            and not self._encoder_used
            and decoder_state == self._new_decoder().getstate()
            and encoder_state
            == codecs.getincrementalencoder(self.encoding)(errors=self.errors).getstate()
        ):
            return 0
        return self._encode_cookie(
            position,
            encoder_state,
            decoder_state,
            self._read_text,
            self._read_eof,
            self._encoder_used,
        )

    def _encode_cookie(
        self,
        position: int,
        encoder_state: int,
        decoder_state: tuple[bytes, int],
        read_text: str,
        read_eof: bool,
        encoder_used: bool,
    ) -> int:
        buffered, decoder_flags = decoder_state
        if (
            isinstance(position, bool)
            or not isinstance(position, int)
            or position < 0
            or isinstance(encoder_state, bool)
            or not isinstance(encoder_state, int)
            or not isinstance(buffered, bytes)
            or isinstance(decoder_flags, bool)
            or not isinstance(decoder_flags, int)
        ):
            raise io.UnsupportedOperation("codec does not support seekable state")
        payload = json.dumps(
            [
                self.binary.mode_name,
                position,
                encoder_state,
                base64.b64encode(buffered).decode("ascii"),
                decoder_flags,
                read_text,
                read_eof,
                encoder_used,
            ],
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("ascii")
        cookie_size = len(_TEXT_COOKIE_PREFIX) + _TEXT_COOKIE_SIGNATURE_SIZE + len(payload)
        if cookie_size > self._max_cookie_bytes:
            raise io.UnsupportedOperation("text position is too large to preserve")
        signature = hmac.digest(self._cookie_secret, payload, hashlib.sha256)
        return int.from_bytes(_TEXT_COOKIE_PREFIX + signature + payload, "big")

    def _decode_cookie(self, cookie: int) -> tuple[int, int, tuple[bytes, int], str, bool, bool]:
        try:
            if (
                isinstance(cookie, bool)
                or not isinstance(cookie, int)
                or cookie < 0
                or cookie.bit_length() > self._max_cookie_bytes * 8
            ):
                raise ValueError
            encoded = cookie.to_bytes(max(1, (cookie.bit_length() + 7) // 8), "big")
            payload_start = len(_TEXT_COOKIE_PREFIX) + _TEXT_COOKIE_SIGNATURE_SIZE
            if not encoded.startswith(_TEXT_COOKIE_PREFIX) or len(encoded) <= payload_start:
                raise ValueError
            signature = encoded[len(_TEXT_COOKIE_PREFIX) : payload_start]
            payload = encoded[payload_start:]
            expected = hmac.digest(self._cookie_secret, payload, hashlib.sha256)
            if not hmac.compare_digest(signature, expected):
                raise ValueError
            value = json.loads(payload)
            if (
                not isinstance(value, list)
                or len(value) != 8
                or value[0] != self.binary.mode_name
                or isinstance(value[1], bool)
                or not isinstance(value[1], int)
                or value[1] < 0
                or isinstance(value[2], bool)
                or not isinstance(value[2], int)
                or not isinstance(value[3], str)
                or isinstance(value[4], bool)
                or not isinstance(value[4], int)
                or not isinstance(value[5], str)
                or not isinstance(value[6], bool)
                or not isinstance(value[7], bool)
            ):
                raise ValueError
            buffered = base64.b64decode(value[3], validate=True)
        except (AttributeError, OverflowError, TypeError, ValueError, json.JSONDecodeError):
            raise io.UnsupportedOperation("can't restore arbitrary text position") from None
        return value[1], value[2], (buffered, value[4]), value[5], value[6], value[7]

    def _validated_cookie(self, cookie: int) -> tuple[int, int, tuple[bytes, int], str, bool, bool]:
        state = self._decode_cookie(cookie)
        position, encoder_state, decoder_state, _, _, _ = state
        engine = self.binary._engine
        if isinstance(engine, _MutationEngine) and position > engine.size:
            raise io.UnsupportedOperation("can't restore arbitrary text position")
        try:
            candidate_decoder = self._new_decoder()
            candidate_decoder.setstate(decoder_state)
            if candidate_decoder.getstate() != decoder_state:
                raise ValueError
        except (OverflowError, TypeError, ValueError):
            raise io.UnsupportedOperation("can't restore invalid decoder state") from None
        try:
            candidate_encoder = codecs.getincrementalencoder(self.encoding)(errors=self.errors)
            candidate_encoder.setstate(encoder_state)
            if candidate_encoder.getstate() != encoder_state:
                raise ValueError
        except (OverflowError, TypeError, ValueError):
            raise io.UnsupportedOperation("can't restore invalid encoder state") from None
        return state

    def _translate(self, text: str) -> str:
        target = os.linesep if self.newline is None else self.newline
        return text if target in ("", "\n") else text.replace("\n", target)

    async def write(self, text: str) -> int:
        """Encode and stage text for publication.

        Args:
            text: Text to write.

        Returns:
            Number of input characters accepted.
        """
        if not isinstance(text, str):
            raise TypeError("write() argument must be str")
        if not text:
            self.binary._check_io()
            return 0
        self._reset_reader()
        data = self._encoder.encode(self._translate(text), final=False)
        self._encoder_used = True
        self._last_operation = "write"
        if data:
            await self.binary.write(data)
        return len(text)

    async def _fill(self, stop_when: Callable[[str], bool] | None = None) -> None:
        if self._read_eof:
            return
        data = await self.binary.readline(_READ_CHUNK_SIZE)
        if data:
            for index, byte in enumerate(data):
                decoded = self._decoder.decode(bytes((byte,)), final=False)
                self._read_text += decoded
                if decoded:
                    self._encoder.encode(decoded, final=False)
                if stop_when is not None and stop_when(decoded):
                    unread = len(data) - index - 1
                    if unread:
                        await self.binary.seek(-unread, io.SEEK_CUR)
                    return
        else:
            decoded = self._decoder.decode(b"", final=True)
            self._read_text += decoded
            if decoded:
                self._encoder.encode(decoded, final=False)
            self._read_eof = True

    def _line_end(self) -> int | None:
        if self.newline is None:
            newline = self._read_text.find("\n")
            return None if newline < 0 else newline + 1
        if self.newline == "":
            for index, character in enumerate(self._read_text):
                if character == "\n":
                    return index + 1
                if character == "\r":
                    if index + 1 == len(self._read_text) and not self._read_eof:
                        return None
                    if self._read_text[index + 1 : index + 2] == "\n":
                        return index + 2
                    return index + 1
            return None
        newline = self._read_text.find(self.newline)
        return None if newline < 0 else newline + len(self.newline)

    def _decoded_ends_line(self, decoded: str) -> bool:
        if self.newline is None:
            return "\n" in decoded
        if self.newline == "":
            return "\r" in decoded or "\n" in decoded
        return self._read_text.endswith(self.newline)

    async def read(self, size: int | None = -1) -> str:
        """Read decoded text from the staged file in readable update modes.

        Args:
            size: Maximum number of characters to read, or ``-1`` for all
                remaining text.

        Returns:
            Decoded text.
        """
        if self._last_operation == "write":
            await self.flush()
            self._reset_reader()
        self._last_operation = "read"
        if size is None:
            size = -1
        if size < 0:
            while not self._read_eof:
                await self._fill()
            result, self._read_text = self._read_text, ""
            return result
        while len(self._read_text) < size and not self._read_eof:
            await self._fill(lambda _decoded: len(self._read_text) >= size)
        result, self._read_text = self._read_text[:size], self._read_text[size:]
        return result

    async def readline(self, size: int | None = -1) -> str:
        """Read one decoded line from the staged file.

        Args:
            size: Maximum number of characters to read, or ``-1`` for an
                unbounded line.

        Returns:
            The next line according to the configured newline mode.
        """
        if self._last_operation == "write":
            await self.flush()
            self._reset_reader()
        self._last_operation = "read"
        limit = -1 if size is None else size
        while True:
            line_end = self._line_end()
            if (
                line_end is not None
                or self._read_eof
                or (limit >= 0 and len(self._read_text) >= limit)
            ):
                take = len(self._read_text) if line_end is None else line_end
                if limit >= 0:
                    take = min(take, limit)
                result, self._read_text = self._read_text[:take], self._read_text[take:]
                return result
            await self._fill(
                lambda decoded: (
                    self._decoded_ends_line(decoded)
                    or (limit >= 0 and len(self._read_text) >= limit)
                )
            )

    async def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        """Move the text cursor using a cookie returned by ``tell()``.

        Args:
            offset: Text seek cookie, or zero for the beginning.
            whence: Standard ``io`` seek mode. Only limited text seeks are
                supported.

        Returns:
            The restored cookie.
        """
        if whence == io.SEEK_CUR:
            if offset != 0:
                raise io.UnsupportedOperation("can't do nonzero cur-relative seeks")
            return self.tell()
        if whence == io.SEEK_END:
            if offset != 0:
                raise io.UnsupportedOperation("can't do nonzero end-relative seeks")
            await self.flush()
            await self.binary.seek(0, io.SEEK_END)
            self._encoder.setstate(self._encoder_started_state)
            self._reset_reader()
            self._read_eof = True
            self._encoder_used = False
            self._last_operation = "seek"
            return self.tell()
        if whence != io.SEEK_SET:
            raise ValueError(f"invalid whence ({whence!r})")
        state = None if offset == 0 else self._validated_cookie(offset)
        await self.flush()
        if offset == 0:
            await self.binary.seek(0)
            if self.binary.mode.appending:
                self._encoder.setstate(self._encoder_started_state)
            else:
                self._encoder.reset()
            self._reset_reader()
            self._encoder_used = False
            self._last_operation = "seek"
            return 0
        assert state is not None
        (
            position,
            encoder_state,
            decoder_state,
            read_text,
            read_eof,
            encoder_used,
        ) = state
        await self.binary.seek(position)
        self._encoder.setstate(encoder_state)
        self._decoder.setstate(decoder_state)
        self._read_text = read_text
        self._read_eof = read_eof
        self._encoder_used = encoder_used
        self._last_operation = "seek"
        return offset

    async def truncate(self, size: int | None = None) -> int:
        """Resize the staged text file.

        Args:
            size: Text seek cookie for the target size, zero, or ``None`` for
                the current position.

        Returns:
            New underlying byte size.
        """
        state = None
        if size is not None and size != 0:
            state = self._validated_cookie(size)
        await self.flush()
        if size is None:
            binary_size = self.binary.tell()
        elif size == 0:
            binary_size = 0
        else:
            assert state is not None
            binary_size = state[0]
        result = await self.binary.truncate(binary_size)
        self._reset_reader()
        return result

    async def flush(self) -> None:
        """Flush encoded text to the staged binary writer."""
        if self._encoder_used and not self._finalized:
            data = self._encoder.encode("", final=False)
            if data:
                await self.binary.write(data)
        await self.binary.flush()

    async def close(self) -> None:
        """Finalize encoding, publish staged data, and clear cookie secrets."""
        if self.binary.closed:
            try:
                await self.binary.close()
            finally:
                self._cookie_secret = b""
            return
        try:
            if not self._finalized:
                self._finalized = True
                try:
                    data = self._encoder.encode("", final=True) if self._encoder_used else b""
                    if data:
                        await self.binary.write(data)
                except BaseException as exc:
                    await self.binary._break(exc)
                    raise
            await self.binary.close()
        finally:
            self._cookie_secret = b""

    async def abort(self) -> None:
        """Close staging resources without publishing and clear cookie secrets."""
        try:
            await self.binary.abort()
        finally:
            self._cookie_secret = b""


__all__ = ["BlobTextWriterState", "BlobWriterState", "create_writer_state"]
