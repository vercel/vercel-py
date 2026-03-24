"""Internal filesystem client primitives with runtime-specific platforms.

The shared client delegates to a platform with an async-shaped interface. The
sync platform wraps explicitly synchronous filesystem operations so shared
business logic can still run through ``iter_coroutine()`` without suspension.
The async platform delegates to ``anyio``'s async filesystem primitives.
"""

from __future__ import annotations

import os
from typing import BinaryIO, Generic, Protocol, TypeVar

import anyio

PathInput = str | os.PathLike[str]
SyncFileHandle = BinaryIO
AsyncFileHandle = anyio.AsyncFile[bytes]
FileHandle = SyncFileHandle | AsyncFileHandle
HandleT = TypeVar("HandleT", SyncFileHandle, AsyncFileHandle)


def _coerce_path(path: PathInput) -> str:
    return os.fspath(path)


def _parent_dir(path: PathInput) -> str:
    return os.path.dirname(_coerce_path(path)) or "."


class FilesystemPlatform(Protocol[HandleT]):
    async def coerce_path(self, path: PathInput) -> str: ...

    async def create_parent_directories(self, path: PathInput) -> None: ...

    async def open_binary_writer(self, path: PathInput) -> HandleT: ...

    async def write(self, handle: HandleT, data: bytes) -> None: ...

    async def close(self, handle: HandleT) -> None: ...

    async def replace(self, src: PathInput, dst: PathInput) -> None: ...

    async def remove_if_exists(self, path: PathInput) -> None: ...

    async def exists(self, path: PathInput) -> bool: ...


class SyncFilesystemPlatform:
    """Sync platform with async interface for use with ``iter_coroutine()``."""

    async def coerce_path(self, path: PathInput) -> str:
        return _coerce_path(path)

    async def create_parent_directories(self, path: PathInput) -> None:
        os.makedirs(_parent_dir(path), exist_ok=True)

    async def open_binary_writer(self, path: PathInput) -> SyncFileHandle:
        return open(_coerce_path(path), "wb")

    async def write(self, handle: SyncFileHandle, data: bytes) -> None:
        handle.write(data)

    async def close(self, handle: SyncFileHandle) -> None:
        handle.close()

    async def replace(self, src: PathInput, dst: PathInput) -> None:
        os.replace(_coerce_path(src), _coerce_path(dst))

    async def remove_if_exists(self, path: PathInput) -> None:
        try:
            os.remove(_coerce_path(path))
        except FileNotFoundError:
            pass

    async def exists(self, path: PathInput) -> bool:
        return os.path.exists(_coerce_path(path))


class AsyncFilesystemPlatform:
    async def coerce_path(self, path: PathInput) -> str:
        return _coerce_path(path)

    async def create_parent_directories(self, path: PathInput) -> None:
        await anyio.Path(_parent_dir(path)).mkdir(parents=True, exist_ok=True)

    async def open_binary_writer(self, path: PathInput) -> AsyncFileHandle:
        return await anyio.open_file(_coerce_path(path), "wb")

    async def write(self, handle: AsyncFileHandle, data: bytes) -> None:
        await handle.write(data)

    async def close(self, handle: AsyncFileHandle) -> None:
        await handle.aclose()

    async def replace(self, src: PathInput, dst: PathInput) -> None:
        await anyio.Path(_coerce_path(src)).replace(_coerce_path(dst))

    async def remove_if_exists(self, path: PathInput) -> None:
        await anyio.Path(_coerce_path(path)).unlink(missing_ok=True)

    async def exists(self, path: PathInput) -> bool:
        return await anyio.Path(_coerce_path(path)).exists()


class FilesystemClient(Generic[HandleT]):
    """Shared filesystem client with a transport-backed async API."""

    def __init__(self, *, platform: FilesystemPlatform[HandleT]) -> None:
        self._platform: FilesystemPlatform[HandleT] = platform

    async def coerce_path(self, path: PathInput) -> str:
        return await self._platform.coerce_path(path)

    async def create_parent_directories(self, path: PathInput) -> None:
        await self._platform.create_parent_directories(path)

    async def open_binary_writer(self, path: PathInput) -> HandleT:
        return await self._platform.open_binary_writer(path)

    async def write(self, handle: HandleT, data: bytes) -> None:
        await self._platform.write(handle, data)

    async def close(self, handle: HandleT) -> None:
        await self._platform.close(handle)

    async def replace(self, src: PathInput, dst: PathInput) -> None:
        await self._platform.replace(src, dst)

    async def remove_if_exists(self, path: PathInput) -> None:
        await self._platform.remove_if_exists(path)

    async def exists(self, path: PathInput) -> bool:
        return await self._platform.exists(path)


def create_filesystem_client() -> FilesystemClient[SyncFileHandle]:
    """Create a sync filesystem client backed by blocking file operations."""

    return FilesystemClient(platform=SyncFilesystemPlatform())


def create_async_filesystem_client() -> FilesystemClient[AsyncFileHandle]:
    """Create an async filesystem client backed by anyio filesystem primitives."""

    return FilesystemClient(platform=AsyncFilesystemPlatform())


class SyncFilesystemClient(FilesystemClient[SyncFileHandle]):
    """Convenience wrapper matching the repo's sync/async client naming pattern."""

    def __init__(self) -> None:
        super().__init__(platform=SyncFilesystemPlatform())


class AsyncFilesystemClient(FilesystemClient[AsyncFileHandle]):
    """Convenience wrapper matching the repo's sync/async client naming pattern."""

    def __init__(self) -> None:
        super().__init__(platform=AsyncFilesystemPlatform())


__all__ = [
    "AsyncFileHandle",
    "AsyncFilesystemClient",
    "AsyncFilesystemPlatform",
    "FileHandle",
    "FilesystemClient",
    "FilesystemPlatform",
    "PathInput",
    "SyncFileHandle",
    "SyncFilesystemClient",
    "SyncFilesystemPlatform",
    "create_async_filesystem_client",
    "create_filesystem_client",
]
