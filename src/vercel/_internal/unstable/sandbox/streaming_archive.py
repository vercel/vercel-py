"""Bounded-memory streaming tar+gzip encoder for sandbox filesystem transfers."""

import tarfile
import zlib
from collections.abc import Generator, Iterator
from typing import Protocol

import anyio

from vercel._internal.unstable.sandbox.runtime_common import _validate_file_mode


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


class _ArchiveUpload(Protocol):
    async def write(self, data: bytes) -> None: ...

    async def finish(self) -> None: ...

    async def abort(self) -> None: ...


class ArchiveRequestWriter:
    """Push raw entry data through the archive encoder into one request."""

    def __init__(self, request: _ArchiveUpload, chunk_size: int) -> None:
        self._request = request
        self._encoder = _TarGzipEncoder(chunk_size)
        self._entry_open = False
        self._closed = False

    async def _drain(self) -> None:
        for chunk in self._encoder.drain():
            await self._request.write(chunk)

    async def start_entry(self, path: str, size: int, mode: int | None) -> None:
        if self._closed:
            raise anyio.ClosedResourceError
        self._encoder.add_entry(path, size, mode)
        self._entry_open = True
        await self._drain()

    async def write(self, data: bytes) -> None:
        if self._closed:
            raise anyio.ClosedResourceError
        self._encoder.write_entry_data(data)
        await self._drain()

    async def finish_entry(self) -> None:
        if self._closed:
            raise anyio.ClosedResourceError
        self._encoder.finish_entry()
        self._entry_open = False
        await self._drain()

    async def finish(self) -> None:
        if self._closed:
            raise anyio.ClosedResourceError
        if self._entry_open:
            await self.finish_entry()
        self._closed = True
        await self._drain()
        for chunk in self._encoder.finalize():
            await self._request.write(chunk)
        await self._request.finish()

    async def abort(self) -> None:
        if not self._closed:
            self._closed = True
            await self._request.abort()


class _CurrentEntry:
    __slots__ = ("size", "written")

    def __init__(self, size: int) -> None:
        self.size = size
        self.written = 0
