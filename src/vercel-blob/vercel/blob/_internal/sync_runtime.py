"""Synchronous Blob stream facades."""

from __future__ import annotations

import io
import os
import threading
from datetime import timedelta
from typing import Any

from vercel._internal.core.iter_coroutine import iter_coroutine

from .models import Access, BlobStatResult
from .service import BlobService
from .streams import BinaryReaderCore, BinaryWriterCore, TextReaderCore


class SyncBlobBinaryStream(io.BufferedIOBase):
    def __init__(self, core: BinaryReaderCore) -> None:
        super().__init__()
        self._core = core

    @property
    def name(self) -> str:
        return self._core.stat.pathname

    @property
    def mode(self) -> str:
        return "rb"

    @property
    def stat(self) -> BlobStatResult:
        return self._core.stat

    def readable(self) -> bool:
        self._checkClosed()
        self._core.service.ensure_open()
        return True

    def seekable(self) -> bool:
        self._checkClosed()
        return False

    def tell(self) -> int:
        self._checkClosed()
        self._core.service.ensure_open()
        return self._core.position

    def read(self, size: int | None = -1) -> bytes:
        return iter_coroutine(self._core.read(size))

    def readinto(self, buffer: Any) -> int:
        return iter_coroutine(self._core.readinto(buffer))

    def readline(self, size: int | None = -1) -> bytes:
        return iter_coroutine(self._core.readline(size))

    def close(self) -> None:
        if not self.closed:
            iter_coroutine(self._core.close())
        super().close()


class SyncBlobBinaryWriter(io.BufferedIOBase):
    def __init__(self, core: BinaryWriterCore) -> None:
        super().__init__()
        self._core = core
        self._abort_on_close = False
        self._lock = threading.RLock()
        self._close_lock = threading.Lock()
        self._closing = False

    @property
    def closed(self) -> bool:
        return self._core.closed

    @property
    def name(self) -> str:
        return self._core.pathname

    @property
    def mode(self) -> str:
        return "wb"

    def writable(self) -> bool:
        self._checkClosed()
        self._core.service.ensure_open()
        return True

    def tell(self) -> int:
        self._checkClosed()
        self._core.service.ensure_open()
        return self._core.position

    def write(self, data: Any) -> int:
        with self._lock:
            return iter_coroutine(self._core.write(data))

    def flush(self) -> None:
        with self._lock:
            if self._closing:
                return
            if self.closed:
                raise ValueError("flush of closed file")
            iter_coroutine(self._core.flush())

    def close(self) -> None:
        with self._close_lock:
            self._closing = True
            try:
                with self._lock:
                    if self._core.broken is not None:
                        iter_coroutine(self._core.close())
                    elif not self.closed:
                        if self._abort_on_close:
                            iter_coroutine(self._core.abort())
                        else:
                            iter_coroutine(self._core.close())
            finally:
                try:
                    super().close()
                finally:
                    self._closing = False

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self._abort_on_close = exc_type is not None
        try:
            self.close()
        except BaseException as cleanup_error:
            if exc is None:
                raise
            add_note = getattr(exc, "add_note", None)
            if callable(add_note):
                add_note(f"Blob staging cleanup also failed: {cleanup_error!r}")


class SyncBlobTextStream(io.TextIOBase):
    def __init__(
        self,
        binary: SyncBlobBinaryStream,
        *,
        encoding: str,
        errors: str,
        newline: str | None,
    ) -> None:
        super().__init__()
        self._binary = binary
        self._encoding = encoding
        self._errors = errors
        self._text = TextReaderCore(
            binary._core,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )

    @property
    def name(self) -> str:
        return self._binary.name

    @property
    def mode(self) -> str:
        return "r"

    @property
    def stat(self) -> BlobStatResult:
        return self._binary.stat

    @property
    def encoding(self) -> str:  # type: ignore[override]
        return self._encoding

    @property
    def errors(self) -> str:  # type: ignore[override]
        return self._errors

    @property
    def newlines(self):
        return self._text.newlines

    def readable(self) -> bool:
        self._checkClosed()
        self._binary._core.service.ensure_open()
        return True

    def tell(self) -> int:
        self._checkClosed()
        self._binary._core.service.ensure_open()
        return self._text.tell()

    def read(self, size: int | None = -1) -> str:
        self._checkClosed()
        self._binary._core.service.ensure_open()
        return self._text.read(size)

    def readline(self, size: int | None = -1) -> str:  # type: ignore[override]
        self._checkClosed()
        self._binary._core.service.ensure_open()
        return self._text.readline(size)

    def close(self) -> None:
        if not self.closed:
            self._binary.close()
        super().close()


class SyncBlobTextWriter(io.TextIOBase):
    def __init__(
        self,
        binary: SyncBlobBinaryWriter,
        *,
        encoding: str,
        errors: str,
        newline: str | None,
    ) -> None:
        super().__init__()
        self._binary = binary
        self._encoding = encoding
        self._errors = errors
        self._newline = newline
        self._closing = False

    @property
    def name(self) -> str:
        return self._binary.name

    @property
    def mode(self) -> str:
        return "w"

    @property
    def encoding(self) -> str:  # type: ignore[override]
        return self._encoding

    @property
    def errors(self) -> str:  # type: ignore[override]
        return self._errors

    def writable(self) -> bool:
        self._checkClosed()
        self._binary._core.service.ensure_open()
        return True

    def tell(self) -> int:
        self._checkClosed()
        self._binary._core.service.ensure_open()
        return self._binary.tell()

    def write(self, text: str) -> int:
        self._checkClosed()
        if not isinstance(text, str):
            raise TypeError("write() argument must be str")
        translated = text
        newline = self._newline
        target_newline = os.linesep if newline is None else newline
        if target_newline not in ("", "\n"):
            translated = text.replace("\n", target_newline)
        self._binary.write(translated.encode(self._encoding, self._errors))
        return len(text)

    def flush(self) -> None:
        if self._closing:
            return
        if self.closed or self._binary.closed:
            raise ValueError("flush of closed file")
        self._binary.flush()

    def close(self) -> None:
        self._closing = True
        try:
            if self._binary._core.broken is not None:
                self._binary.close()
            elif not self.closed:
                self._binary.close()
        finally:
            try:
                super().close()
            finally:
                self._closing = False

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self._binary._abort_on_close = exc_type is not None
        try:
            self.close()
        except BaseException as cleanup_error:
            if exc is None:
                raise
            add_note = getattr(exc, "add_note", None)
            if callable(add_note):
                add_note(f"Blob staging cleanup also failed: {cleanup_error!r}")


def open_sync_stream(
    service: BlobService,
    pathname: str,
    mode: str,
    *,
    access: Access,
    encoding: str,
    errors: str,
    newline: str | None,
    content_type: str | None,
    cache_control_max_age: timedelta | None,
):
    if mode.startswith("r"):
        stat, data = iter_coroutine(service.read_all(pathname, access=access))
        binary = SyncBlobBinaryStream(BinaryReaderCore(service=service, stat=stat, data=data))
        if "b" in mode:
            return binary
        return SyncBlobTextStream(binary, encoding=encoding, errors=errors, newline=newline)
    binary_writer = SyncBlobBinaryWriter(
        iter_coroutine(
            BinaryWriterCore.create(
                service=service,
                pathname=pathname,
                access=access,
                content_type=content_type,
                cache_control_max_age=cache_control_max_age,
            )
        )
    )
    if "b" in mode:
        return binary_writer
    return SyncBlobTextWriter(binary_writer, encoding=encoding, errors=errors, newline=newline)
