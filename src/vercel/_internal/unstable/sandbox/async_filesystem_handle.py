"""Asynchronous facades for shared Sandbox file-handle state machines."""

from collections.abc import AsyncIterator, Iterable
from contextlib import AbstractAsyncContextManager
from types import TracebackType

import anyio

from vercel._internal.unstable.sandbox.filesystem_handle_core import (
    BinaryReaderCore,
    BinaryWriterCore,
    TextReaderCore,
    TextWriterCore,
)


class _AsyncHandle:
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


class SandboxBinaryReader(_AsyncHandle, AsyncIterator[bytes]):
    _core: BinaryReaderCore

    def __init__(self, core: BinaryReaderCore) -> None:
        self._core = core
        self._guard = anyio.ResourceGuard("reading from")

    async def __aenter__(self) -> "SandboxBinaryReader":
        await self._core.enter()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            await self.aclose()
        except BaseException:
            if exc_type is None:
                raise

    async def read(self, size: int = -1) -> bytes:
        with self._guard:
            return await self._core.read(size)

    async def readline(self, size: int = -1) -> bytes:
        with self._guard:
            return await self._core.readline(size)

    async def readinto(self, buffer: object) -> int:
        with self._guard:
            return await self._core.readinto(buffer)

    def __aiter__(self) -> "SandboxBinaryReader":
        return self

    async def __anext__(self) -> bytes:
        line = await self.readline()
        if not line:
            raise StopAsyncIteration
        return line

    async def aclose(self) -> None:
        await self._core.close()


class SandboxTextReader(_AsyncHandle, AsyncIterator[str]):
    _core: TextReaderCore

    def __init__(self, core: TextReaderCore) -> None:
        self._core = core
        self._guard = anyio.ResourceGuard("reading from")

    async def __aenter__(self) -> "SandboxTextReader":
        await self._core.enter()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            await self.aclose()
        except BaseException:
            if exc_type is None:
                raise

    async def read(self, size: int = -1) -> str:
        with self._guard:
            return await self._core.read(size)

    async def readline(self, size: int = -1) -> str:
        with self._guard:
            return await self._core.readline(size)

    def __aiter__(self) -> "SandboxTextReader":
        return self

    async def __anext__(self) -> str:
        line = await self.readline()
        if not line:
            raise StopAsyncIteration
        return line

    async def aclose(self) -> None:
        await self._core.close()


class SandboxBinaryWriter(_AsyncHandle):
    _core: BinaryWriterCore

    def __init__(self, core: BinaryWriterCore) -> None:
        self._core = core
        self._guard = anyio.ResourceGuard("writing to")
        self._lifecycle: AbstractAsyncContextManager[None] | None = None

    async def __aenter__(self) -> "SandboxBinaryWriter":
        lifecycle = self._core.lifecycle()
        self._lifecycle = lifecycle
        try:
            await lifecycle.__aenter__()
        except BaseException:
            self._lifecycle = None
            raise
        return self

    async def __aexit__(
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
                with anyio.CancelScope(shield=True):
                    await lifecycle.__aexit__(exc_type, exc, traceback)
            except BaseException:
                pass
        else:
            await lifecycle.__aexit__(None, None, None)

    async def write(self, data: bytes, /) -> int:
        with self._guard:
            return await self._core.write(data)

    async def writelines(self, lines: Iterable[bytes], /) -> None:
        for line in lines:
            await self.write(line)

    async def flush(self) -> None:
        with self._guard:
            await self._core.flush()

    async def aclose(self) -> None:
        lifecycle, self._lifecycle = self._lifecycle, None
        if lifecycle is None:
            await self._core.close()
        else:
            await lifecycle.__aexit__(None, None, None)


class SandboxTextWriter(_AsyncHandle):
    _core: TextWriterCore

    def __init__(self, core: TextWriterCore) -> None:
        self._core = core
        self._guard = anyio.ResourceGuard("writing to")
        self._lifecycle: AbstractAsyncContextManager[None] | None = None

    async def __aenter__(self) -> "SandboxTextWriter":
        lifecycle = self._core.lifecycle()
        self._lifecycle = lifecycle
        try:
            await lifecycle.__aenter__()
        except BaseException:
            self._lifecycle = None
            raise
        return self

    async def __aexit__(
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
                with anyio.CancelScope(shield=True):
                    await lifecycle.__aexit__(exc_type, exc, traceback)
            except BaseException:
                pass
        else:
            await lifecycle.__aexit__(None, None, None)

    async def write(self, text: str, /) -> int:
        with self._guard:
            return await self._core.write(text)

    async def writelines(self, lines: Iterable[str], /) -> None:
        for line in lines:
            await self.write(line)

    async def flush(self) -> None:
        with self._guard:
            await self._core.flush()

    async def aclose(self) -> None:
        lifecycle, self._lifecycle = self._lifecycle, None
        if lifecycle is None:
            await self._core.close()
        else:
            await lifecycle.__aexit__(None, None, None)
