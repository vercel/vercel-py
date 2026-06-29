"""Synchronous facades for shared Sandbox file-handle state machines."""

from collections.abc import Iterable, Iterator
from contextlib import AbstractAsyncContextManager
from types import TracebackType

from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.unstable.sandbox.filesystem_handle_core import (
    BinaryReaderCore,
    BinaryWriterCore,
    TextReaderCore,
    TextWriterCore,
)


class _SyncHandle:
    _core: BinaryReaderCore | TextReaderCore | BinaryWriterCore | TextWriterCore

    @property
    def name(self) -> str:
        return self._core.name

    @property
    def mode(self) -> str:
        return self._core.mode

    @property
    def closed(self) -> bool:
        return self._core.closed

    def readable(self) -> bool:
        return self._core.readable()

    def writable(self) -> bool:
        return self._core.writable()

    def seekable(self) -> bool:
        return False


class SyncSandboxBinaryReader(_SyncHandle, Iterator[bytes]):
    _core: BinaryReaderCore

    def __init__(self, core: BinaryReaderCore) -> None:
        self._core = core

    def __enter__(self) -> "SyncSandboxBinaryReader":
        iter_coroutine(self._core.enter())
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            self.close()
        except BaseException:
            if exc_type is None:
                raise

    def read(self, size: int = -1) -> bytes:
        return iter_coroutine(self._core.read(size))

    def readline(self, size: int = -1) -> bytes:
        return iter_coroutine(self._core.readline(size))

    def readinto(self, buffer: object) -> int:
        return iter_coroutine(self._core.readinto(buffer))

    def __iter__(self) -> "SyncSandboxBinaryReader":
        return self

    def __next__(self) -> bytes:
        line = self.readline()
        if not line:
            raise StopIteration
        return line

    def close(self) -> None:
        iter_coroutine(self._core.close())


class SyncSandboxTextReader(_SyncHandle, Iterator[str]):
    _core: TextReaderCore

    def __init__(self, core: TextReaderCore) -> None:
        self._core = core

    def __enter__(self) -> "SyncSandboxTextReader":
        iter_coroutine(self._core.enter())
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            self.close()
        except BaseException:
            if exc_type is None:
                raise

    def read(self, size: int = -1) -> str:
        return iter_coroutine(self._core.read(size))

    def readline(self, size: int = -1) -> str:
        return iter_coroutine(self._core.readline(size))

    def __iter__(self) -> "SyncSandboxTextReader":
        return self

    def __next__(self) -> str:
        line = self.readline()
        if not line:
            raise StopIteration
        return line

    def close(self) -> None:
        iter_coroutine(self._core.close())


class SyncSandboxBinaryWriter(_SyncHandle):
    _core: BinaryWriterCore

    def __init__(self, core: BinaryWriterCore) -> None:
        self._core = core
        self._lifecycle: AbstractAsyncContextManager[None] | None = None

    def __enter__(self) -> "SyncSandboxBinaryWriter":
        lifecycle = self._core.lifecycle()
        self._lifecycle = lifecycle
        try:
            iter_coroutine(lifecycle.__aenter__())
        except BaseException:
            self._lifecycle = None
            raise
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        lifecycle, self._lifecycle = self._lifecycle, None
        if lifecycle is None:
            return
        if exc_type is not None:
            try:
                iter_coroutine(lifecycle.__aexit__(exc_type, exc, traceback))
            except BaseException:
                pass
        else:
            iter_coroutine(lifecycle.__aexit__(None, None, None))

    def write(self, data: bytes, /) -> int:
        return iter_coroutine(self._core.write(data))

    def writelines(self, lines: Iterable[bytes], /) -> None:
        for line in lines:
            self.write(line)

    def flush(self) -> None:
        iter_coroutine(self._core.flush())

    def close(self) -> None:
        lifecycle, self._lifecycle = self._lifecycle, None
        if lifecycle is None:
            iter_coroutine(self._core.close())
        else:
            iter_coroutine(lifecycle.__aexit__(None, None, None))


class SyncSandboxTextWriter(_SyncHandle):
    _core: TextWriterCore

    def __init__(self, core: TextWriterCore) -> None:
        self._core = core
        self._lifecycle: AbstractAsyncContextManager[None] | None = None

    def __enter__(self) -> "SyncSandboxTextWriter":
        lifecycle = self._core.lifecycle()
        self._lifecycle = lifecycle
        try:
            iter_coroutine(lifecycle.__aenter__())
        except BaseException:
            self._lifecycle = None
            raise
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        lifecycle, self._lifecycle = self._lifecycle, None
        if lifecycle is None:
            return
        if exc_type is not None:
            try:
                iter_coroutine(lifecycle.__aexit__(exc_type, exc, traceback))
            except BaseException:
                pass
        else:
            iter_coroutine(lifecycle.__aexit__(None, None, None))

    def write(self, text: str, /) -> int:
        return iter_coroutine(self._core.write(text))

    def writelines(self, lines: Iterable[str], /) -> None:
        for line in lines:
            self.write(line)

    def flush(self) -> None:
        iter_coroutine(self._core.flush())

    def close(self) -> None:
        lifecycle, self._lifecycle = self._lifecycle, None
        if lifecycle is None:
            iter_coroutine(self._core.close())
        else:
            iter_coroutine(lifecycle.__aexit__(None, None, None))
