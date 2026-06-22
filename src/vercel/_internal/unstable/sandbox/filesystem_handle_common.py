"""Pure helpers shared by synchronous and asynchronous sandbox file handles."""

import codecs
import io
import os
from enum import Enum, auto
from typing import Any, Literal, TypeAlias

from vercel._internal.unstable.sandbox.errors import SandboxUploadSizeMismatchError
from vercel._internal.unstable.sandbox.runtime_common import (
    RemotePath,
    _coerce_remote_path,
    _validate_file_mode,
    _validate_transfer_size,
)

OpenMode: TypeAlias = Literal["r", "rb", "w", "wb"]


class _HandleState(Enum):
    CREATED = auto()
    ACTIVE = auto()
    CLOSED = auto()


def _validate_open_options(
    path: RemotePath,
    mode: str,
    *,
    encoding: str | None,
    errors: str | None,
    newline: str | None,
    size: int | None,
    permissions: int | None,
) -> tuple[str, OpenMode, str, str, str | None, int | None, int | None]:
    normalized_path = _coerce_remote_path(path)
    if mode not in ("r", "rb", "w", "wb"):
        raise ValueError("mode must be 'r', 'rb', 'w', or 'wb'")
    binary = "b" in mode
    if binary and (encoding is not None or errors is not None or newline is not None):
        raise ValueError("encoding, errors, and newline are not supported in binary mode")
    if newline not in (None, "", "\n", "\r", "\r\n"):
        raise ValueError("illegal newline value")
    if size is not None:
        if mode != "wb":
            raise ValueError("size is only supported in 'wb' mode")
        size = _validate_transfer_size(size)
    if permissions is not None:
        if mode.startswith("r"):
            raise ValueError("permissions are not supported in read mode")
        permissions = _validate_file_mode(permissions)
    resolved_encoding = "utf-8" if encoding is None else encoding
    resolved_errors = "strict" if errors is None else errors
    if not binary:
        codecs.lookup(resolved_encoding)
        codecs.lookup_error(resolved_errors)
    return (
        normalized_path,
        mode,  # type: ignore[return-value]
        resolved_encoding,
        resolved_errors,
        newline,
        size,
        permissions,
    )


def _validate_read_size(size: int) -> None:
    if not isinstance(size, int):
        raise TypeError("size must be an integer")
    if size < -1:
        raise ValueError("size must be -1 or non-negative")


class _ExactSizeValidator:
    __slots__ = ("_declared", "_name", "_written")

    def __init__(self, name: str, declared: int) -> None:
        self._name = name
        self._declared = declared
        self._written = 0

    def validate_write(self, size: int) -> None:
        consumed = self._written + size
        if consumed > self._declared:
            raise SandboxUploadSizeMismatchError(
                self._name,
                declared=self._declared,
                consumed=consumed,
                early_end=False,
            )

    def record_write(self, size: int) -> None:
        self._written += size

    def validate_close(self) -> None:
        if self._written != self._declared:
            raise SandboxUploadSizeMismatchError(
                self._name,
                declared=self._declared,
                consumed=self._written,
                early_end=True,
            )


class _HandleInfo:
    __slots__ = ("_state", "mode", "name")

    def __init__(self, name: str, mode: OpenMode) -> None:
        self.name = name
        self.mode = mode
        self._state = _HandleState.CREATED

    @property
    def closed(self) -> bool:
        return self._state is _HandleState.CLOSED

    def readable(self) -> bool:
        return self.mode.startswith("r")

    def writable(self) -> bool:
        return self.mode.startswith("w")

    def seekable(self) -> bool:
        return False

    def _enter(self) -> None:
        if self._state is not _HandleState.CREATED:
            raise ValueError("I/O operation on closed or already-entered file")
        self._state = _HandleState.ACTIVE

    def _ensure_active(self) -> None:
        if self._state is not _HandleState.ACTIVE:
            raise ValueError("I/O operation on closed or unopened file")

    def _mark_closed(self) -> None:
        self._state = _HandleState.CLOSED


class _TextReadBuffer:
    __slots__ = ("_buffer", "_decoder", "_eof", "_newline")

    def __init__(self, encoding: str, errors: str, newline: str | None) -> None:
        decoder: Any = codecs.getincrementaldecoder(encoding)(errors)
        if newline in (None, ""):
            decoder = io.IncrementalNewlineDecoder(decoder, newline is None)
        self._decoder = decoder
        self._newline = newline
        self._buffer = ""
        self._eof = False

    def feed(self, data: bytes, *, final: bool = False) -> None:
        self._buffer += self._decoder.decode(data, final)
        self._eof = final

    def take(self, size: int) -> str:
        if size < 0:
            result, self._buffer = self._buffer, ""
        else:
            result, self._buffer = self._buffer[:size], self._buffer[size:]
        return result

    def line_end(self, size: int = -1) -> int | None:
        limit = len(self._buffer) if size < 0 else min(size, len(self._buffer))
        text = self._buffer[:limit]
        if self._newline is None:
            index = text.find("\n")
            return None if index < 0 else index + 1
        if self._newline == "":
            for index, char in enumerate(text):
                if char == "\n":
                    return index + 1
                if char == "\r":
                    if index + 1 < len(text):
                        return index + (2 if text[index + 1] == "\n" else 1)
                    if self._eof or limit < len(self._buffer):
                        return index + 1
                    return None
            return None
        index = text.find(self._newline)
        return None if index < 0 else index + len(self._newline)


class _TextEncoder:
    __slots__ = ("_encoder", "_newline")

    def __init__(self, encoding: str, errors: str, newline: str | None) -> None:
        self._encoder = codecs.getincrementalencoder(encoding)(errors)
        self._newline = os.linesep if newline is None else newline

    def encode(self, text: str, *, final: bool = False) -> bytes:
        if not isinstance(text, str):
            raise TypeError(f"write() argument must be str, not {type(text).__name__}")
        if self._newline not in ("", "\n"):
            text = text.replace("\n", self._newline)
        return self._encoder.encode(text, final)
