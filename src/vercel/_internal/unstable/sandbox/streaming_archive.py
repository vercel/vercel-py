"""Bounded-memory streaming tar+gzip encoder for sandbox filesystem transfers."""

import tarfile
import typing
import zlib
from collections.abc import AsyncIterator, Generator, Iterator

from vercel._internal.unstable.sandbox.errors import SandboxUploadSizeMismatchError
from vercel._internal.unstable.sandbox.runtime_common import (
    _AsyncReadableBytes,
    _ReadableBytes,
    _UploadFileEntry,
    _validate_file_mode,
)


class _TarGzipEncoder:
    """Synchronous state machine that builds a gzipped tar archive chunk by chunk.

    Each entry is added sequentially: call ``add_entry``, feed data with
    ``write_entry_data``, then ``finish_entry``. Compressed chunks can be drained
    at any time via ``next_chunk``. After all entries, call ``finalize`` to get
    the remaining chunks (including the trailer and gzip flush).
    """

    __slots__ = ("_chunk_size", "_compressor", "_buffer", "_current_entry", "_finalized")

    def __init__(self, chunk_size: int) -> None:
        if chunk_size < 1:
            raise ValueError("chunk_size must be a positive integer")
        self._chunk_size = chunk_size
        self._compressor = zlib.compressobj(wbits=31)
        self._buffer: bytearray = bytearray()
        self._current_entry: _CurrentEntry | None = None
        self._finalized = False

    def add_entry(self, path: str, size: int, mode: int | None = None) -> int:
        if self._finalized:
            raise RuntimeError("Encoder already finalized")
        if self._current_entry is not None:
            raise RuntimeError("Previous entry not finished")

        info = tarfile.TarInfo(name=path)
        info.size = size
        normalized_mode = _validate_file_mode(mode)
        info.mode = 0o644 if normalized_mode is None else normalized_mode
        info.uid = 0
        info.gid = 0
        info.mtime = 0
        info.uname = ""
        info.gname = ""

        header = info.tobuf(format=tarfile.PAX_FORMAT)
        self._compress(header)
        self._current_entry = _CurrentEntry(size=size)
        return size

    def write_entry_data(self, data: bytes) -> None:
        if self._current_entry is None:
            raise RuntimeError("No active entry")
        entry = self._current_entry
        if entry.written + len(data) > entry.size:
            raise ValueError("Trailing data: would exceed declared entry size")
        self._compress(data)
        entry.written += len(data)

    def finish_entry(self) -> None:
        if self._current_entry is None:
            raise RuntimeError("No active entry")
        entry = self._current_entry
        if entry.written < entry.size:
            raise ValueError("Early end: entry not fully written")

        remainder = entry.size % 512
        if remainder > 0:
            self._compress(b"\0" * (512 - remainder))

        self._current_entry = None

    def finalize(self) -> Iterator[bytes]:
        if self._finalized:
            raise RuntimeError("Encoder already finalized")
        if self._current_entry is not None:
            raise RuntimeError("Cannot finalize with active entry")

        self._finalized = True
        self._compress(b"\0" * 1024)

        flushed = self._compressor.flush()
        if flushed:
            self._buffer.extend(flushed)

        chunks = list(self.drain())
        if self._buffer:
            chunks.append(bytes(self._buffer))
            self._buffer.clear()
        return iter(chunks)

    def drain(self) -> Generator[bytes, None, None]:
        """Drain all output currently available in bounded chunks."""
        while len(self._buffer) >= self._chunk_size:
            chunk = bytes(self._buffer[: self._chunk_size])
            del self._buffer[: self._chunk_size]
            yield chunk
        if self._buffer:
            yield bytes(self._buffer)
            self._buffer.clear()

    def next_chunk(self) -> bytes | None:
        return next(self.drain(), None)

    def _compress(self, data: bytes) -> None:
        compressed = self._compressor.compress(data)
        if compressed:
            self._buffer.extend(compressed)


class _CurrentEntry:
    __slots__ = ("size", "written")

    def __init__(self, size: int) -> None:
        self.size = size
        self.written = 0


class _BytesReader:
    __slots__ = ("_view", "_offset")

    def __init__(self, data: bytes) -> None:
        self._view = memoryview(data)
        self._offset = 0

    def read(self, size: int = -1) -> bytes:
        remaining = self._view[self._offset :]
        if size < 0:
            result = bytes(remaining)
            self._offset = len(self._view)
            return result
        chunk = remaining[:size]
        result = bytes(chunk)
        self._offset += len(result)
        return result


def _coerce_to_bytes(chunk: object) -> bytes:
    if isinstance(chunk, bytes):
        return chunk
    raise TypeError(f"Source produced non-bytes chunk of type {type(chunk).__name__}")


def _make_reader(source: object) -> _BytesReader:
    if isinstance(source, bytes):
        return _BytesReader(source)
    if isinstance(source, memoryview):
        return _BytesReader(bytes(source))
    if isinstance(source, bytearray):
        return _BytesReader(bytes(source))
    raise TypeError(f"_UploadFileEntry source is not bytes: {type(source).__name__}")


async def _read_async(source: _AsyncReadableBytes, n: int) -> bytes:
    raw = await source.read(n)
    return _coerce_to_bytes(raw)


def _read_sync(source: _ReadableBytes, n: int) -> bytes:
    raw = source.read(n)
    return _coerce_to_bytes(raw)


def _is_bytes_source(source: object) -> bool:
    return isinstance(source, (bytes, memoryview, bytearray))


async def async_archive_body(
    entries: list[_UploadFileEntry], chunk_size: int
) -> AsyncIterator[bytes]:
    encoder = _TarGzipEncoder(chunk_size)
    for entry in entries:
        encoder.add_entry(entry.archive_path or entry.path, entry.size, entry.mode)
        for chunk in encoder.drain():
            yield chunk
        bytes_source = _is_bytes_source(entry.source)
        if bytes_source:
            bytes_reader = _make_reader(entry.source)
        remaining = entry.size
        while remaining > 0:
            n = min(chunk_size, remaining)
            if bytes_source:
                data = _coerce_to_bytes(bytes_reader.read(n))
            else:
                data = await _read_async(typing.cast(_AsyncReadableBytes, entry.source), n)
            if not data:
                raise SandboxUploadSizeMismatchError(
                    entry.path,
                    declared=entry.size,
                    consumed=entry.size - remaining,
                    early_end=True,
                )
            consumed = entry.size - remaining + len(data)
            if len(data) > remaining:
                raise SandboxUploadSizeMismatchError(
                    entry.path, declared=entry.size, consumed=consumed, early_end=False
                )
            encoder.write_entry_data(data)
            remaining -= len(data)
            for chunk in encoder.drain():
                yield chunk
        if bytes_source:
            trailing = bytes_reader.read(1)
        else:
            trailing = await _read_async(typing.cast(_AsyncReadableBytes, entry.source), 1)
        if trailing:
            raise SandboxUploadSizeMismatchError(
                entry.path,
                declared=entry.size,
                consumed=entry.size + len(trailing),
                early_end=False,
            )
        encoder.finish_entry()
        for chunk in encoder.drain():
            yield chunk

    for chunk in encoder.finalize():
        yield chunk


def sync_archive_body(entries: list[_UploadFileEntry], chunk_size: int) -> Iterator[bytes]:
    encoder = _TarGzipEncoder(chunk_size)
    for entry in entries:
        encoder.add_entry(entry.archive_path or entry.path, entry.size, entry.mode)
        yield from encoder.drain()
        bytes_source = _is_bytes_source(entry.source)
        if bytes_source:
            bytes_reader = _make_reader(entry.source)
        remaining = entry.size
        while remaining > 0:
            n = min(chunk_size, remaining)
            if bytes_source:
                data = _coerce_to_bytes(bytes_reader.read(n))
            else:
                data = _read_sync(typing.cast(_ReadableBytes, entry.source), n)
            if not data:
                raise SandboxUploadSizeMismatchError(
                    entry.path,
                    declared=entry.size,
                    consumed=entry.size - remaining,
                    early_end=True,
                )
            consumed = entry.size - remaining + len(data)
            if len(data) > remaining:
                raise SandboxUploadSizeMismatchError(
                    entry.path, declared=entry.size, consumed=consumed, early_end=False
                )
            encoder.write_entry_data(data)
            remaining -= len(data)
            yield from encoder.drain()
        if bytes_source:
            trailing = bytes_reader.read(1)
        else:
            trailing = _read_sync(typing.cast(_ReadableBytes, entry.source), 1)
        if trailing:
            raise SandboxUploadSizeMismatchError(
                entry.path,
                declared=entry.size,
                consumed=entry.size + len(trailing),
                early_end=False,
            )
        encoder.finish_entry()
        yield from encoder.drain()

    yield from encoder.finalize()
