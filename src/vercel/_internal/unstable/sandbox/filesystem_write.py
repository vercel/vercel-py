"""Write targets for streaming Sandbox filesystem handles."""

from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Protocol

from vercel._internal.byte_stream import (
    ReadableByteStream,
    StagingByteFile,
)
from vercel._internal.unstable.sandbox.filesystem_handle_common import (
    _ExactSizeValidator,
)
from vercel._internal.unstable.sandbox.runtime_common import _UploadFileEntry
from vercel._internal.unstable.sandbox.service import SandboxArchiveUpload, SandboxService


class _WriteTarget(Protocol):
    async def write(self, data: bytes) -> None: ...

    async def flush(self) -> None: ...

    async def finish(self) -> None: ...

    async def abort(self) -> None: ...


class _WriteTargetSource(Protocol):
    name: str

    def acquire(self) -> AbstractAsyncContextManager[_WriteTarget]: ...


class _WriteBinding(Protocol):
    async def publish(self, source: ReadableByteStream, size: int, mode: int | None) -> None: ...

    def open_upload(
        self, size: int, mode: int | None
    ) -> AbstractAsyncContextManager[_WriteTarget]: ...


class _TemporaryFileRuntime(Protocol):
    def temporary_file(self) -> AbstractAsyncContextManager[StagingByteFile]: ...


class _BoundWrite:
    def __init__(
        self,
        *,
        service: SandboxService,
        session_id: str,
        path: str,
        cwd: str,
        archive_path: str,
    ) -> None:
        self._service = service
        self._session_id = session_id
        self._path = path
        self._cwd = cwd
        self._archive_path = archive_path

    async def publish(self, source: ReadableByteStream, size: int, mode: int | None) -> None:
        await self._service.write_stream_archive(
            session_id=self._session_id,
            entries=[
                _UploadFileEntry(
                    path=self._path,
                    size=size,
                    source=source,
                    mode=mode,
                    archive_path=self._archive_path,
                )
            ],
            paths=(self._path,),
            cwd=self._cwd,
        )

    def open_upload(
        self, size: int, mode: int | None
    ) -> AbstractAsyncContextManager[SandboxArchiveUpload]:
        @asynccontextmanager
        async def open_upload() -> AsyncIterator[SandboxArchiveUpload]:
            async with self._service.open_archive_upload(
                session_id=self._session_id,
                paths=(self._path,),
                cwd=self._cwd,
            ) as upload:
                await upload.start_entry(self._archive_path, size, mode)
                yield upload

        return open_upload()


class _FilesystemWriteTargetSource:
    def __init__(
        self,
        *,
        name: str,
        runtime: _TemporaryFileRuntime,
        bind: Callable[[], _WriteBinding],
        size: int | None,
        permissions: int | None,
    ) -> None:
        self.name = name
        self._runtime = runtime
        self._bind = bind
        self._size = size
        self._permissions = permissions

    def acquire(self) -> AbstractAsyncContextManager[_WriteTarget]:
        return _acquire_write_target(
            runtime=self._runtime,
            bound=self._bind(),
            name=self.name,
            size=self._size,
            permissions=self._permissions,
        )


class _SpooledWriteTarget:
    def __init__(
        self,
        spool: StagingByteFile,
        bound: _WriteBinding,
        permissions: int | None,
    ) -> None:
        self._spool = spool
        self._bound = bound
        self._permissions = permissions

    async def write(self, data: bytes) -> None:
        await self._spool.write(data)

    async def flush(self) -> None:
        await self._spool.flush()

    async def finish(self) -> None:
        await self._spool.flush()
        size = await self._spool.tell()
        await self._spool.seek(0)
        await self._bound.publish(self._spool, size, self._permissions)

    async def abort(self) -> None:
        pass


class _ExactSizeWriteTarget:
    def __init__(self, target: _WriteTarget, *, name: str, size: int) -> None:
        self._target = target
        self._validator = _ExactSizeValidator(name, size)

    async def write(self, data: bytes) -> None:
        self._validator.validate_write(len(data))
        await self._target.write(data)
        self._validator.record_write(len(data))

    async def flush(self) -> None:
        await self._target.flush()

    async def finish(self) -> None:
        self._validator.validate_close()
        await self._target.finish()

    async def abort(self) -> None:
        await self._target.abort()


@asynccontextmanager
async def _acquire_write_target(
    *,
    runtime: _TemporaryFileRuntime,
    bound: _WriteBinding,
    name: str,
    size: int | None,
    permissions: int | None,
) -> AsyncIterator[_WriteTarget]:
    if size is None:
        async with runtime.temporary_file() as spool:
            yield _SpooledWriteTarget(spool, bound, permissions)
    else:
        async with bound.open_upload(size, permissions) as upload:
            yield _ExactSizeWriteTarget(upload, name=name, size=size)
