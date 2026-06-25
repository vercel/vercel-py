from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest
from hypothesis import given, settings, strategies as st
from typing_extensions import Buffer

from vercel._internal.byte_stream import ReadableByteStream, StagingByteFile
from vercel._internal.unstable.sandbox.filesystem_handle_core import BinaryWriterCore
from vercel._internal.unstable.sandbox.filesystem_write import (
    _acquire_write_target,
    _ExactSizeWriteTarget,
    _WriteTarget,
    _WriteTargetSource,
)
from vercel.unstable.sandbox import SandboxUploadSizeMismatchError


class _MemorySpool:
    def __init__(self) -> None:
        self.data = bytearray()
        self.offset = 0
        self.flushes = 0
        self.released = False

    async def read(self, size: int = -1, /) -> bytes:
        end = len(self.data) if size < 0 else self.offset + size
        chunk = bytes(self.data[self.offset : end])
        self.offset += len(chunk)
        return chunk

    async def write(self, data: bytes, /) -> int:
        self.data[self.offset : self.offset + len(data)] = data
        self.offset += len(data)
        return len(data)

    async def readinto(self, buffer: Buffer, /) -> int:
        view = memoryview(buffer)
        data = await self.read(view.nbytes)
        view.cast("B")[: len(data)] = data
        return len(data)

    async def flush(self) -> None:
        self.flushes += 1

    async def tell(self) -> int:
        return self.offset

    async def seek(self, offset: int, whence: int = 0, /) -> int:
        assert whence == 0
        self.offset = offset
        return offset

    async def truncate(self, size: int | None = None, /) -> int:
        final_size = self.offset if size is None else size
        del self.data[final_size:]
        if len(self.data) < final_size:
            self.data.extend(b"\0" * (final_size - len(self.data)))
        return final_size


class _FakeUpload:
    def __init__(self) -> None:
        self.writes: list[bytes] = []
        self.flushes = 0
        self.finishes = 0
        self.aborts = 0
        self.released = False
        self.write_error: BaseException | None = None

    async def write(self, data: bytes) -> None:
        if self.write_error is not None:
            raise self.write_error
        self.writes.append(data)

    async def flush(self) -> None:
        self.flushes += 1

    async def finish(self) -> None:
        self.finishes += 1

    async def abort(self) -> None:
        self.aborts += 1


class _FakeBound:
    def __init__(self, upload: _FakeUpload | None = None) -> None:
        self.upload = _FakeUpload() if upload is None else upload
        self.published: list[tuple[bytes, int, int | None]] = []
        self.opened: list[tuple[int, int | None]] = []

    async def publish(self, source: ReadableByteStream, size: int, mode: int | None) -> None:
        self.published.append((await source.read(), size, mode))

    @asynccontextmanager
    async def open_upload(self, size: int, mode: int | None) -> AsyncIterator[_FakeUpload]:
        self.opened.append((size, mode))
        try:
            yield self.upload
        finally:
            self.upload.released = True


class _FakeRuntime:
    def __init__(self, spool: _MemorySpool) -> None:
        self.spool = spool

    @asynccontextmanager
    async def temporary_file(self) -> AsyncIterator[StagingByteFile]:
        try:
            yield self.spool
        finally:
            self.spool.released = True


@pytest.mark.asyncio
async def test_spooled_target_abort_does_not_publish_and_releases_spool() -> None:
    spool = _MemorySpool()
    bound = _FakeBound()

    async with _acquire_write_target(
        runtime=_FakeRuntime(spool),
        bound=bound,
        name="data.bin",
        size=None,
        permissions=None,
    ) as target:
        await target.write(b"local")
        await target.abort()

    assert bound.published == []
    assert spool.released


@pytest.mark.asyncio
async def test_direct_target_streams_and_finishes_once() -> None:
    bound = _FakeBound()

    async with _acquire_write_target(
        runtime=_FakeRuntime(_MemorySpool()),
        bound=bound,
        name="data.bin",
        size=4,
        permissions=0o640,
    ) as target:
        await target.write(b"data")
        assert bound.upload.writes == [b"data"]
        await target.finish()

    assert bound.upload.finishes == 1
    assert bound.upload.aborts == 0
    assert bound.upload.released
    assert bound.opened == [(4, 0o640)]


@pytest.mark.asyncio
async def test_direct_target_abort_reaches_upload_without_finishing() -> None:
    bound = _FakeBound()

    async with _acquire_write_target(
        runtime=_FakeRuntime(_MemorySpool()),
        bound=bound,
        name="data.bin",
        size=4,
        permissions=None,
    ) as target:
        await target.write(b"data")
        await target.abort()

    assert bound.upload.finishes == 0
    assert bound.upload.aborts == 1


@pytest.mark.asyncio
@settings(deadline=None)
@given(
    declared=st.integers(min_value=0, max_value=64),
    writes=st.lists(
        st.tuples(st.binary(max_size=16), st.booleans()),
        max_size=8,
    ),
)
async def test_exact_size_target_accounting(
    declared: int,
    writes: list[tuple[bytes, bool]],
) -> None:
    upload = _FakeUpload()
    target = _ExactSizeWriteTarget(upload, name="data.bin", size=declared)
    forwarded: list[bytes] = []
    consumed = 0

    for chunk, inject_failure in writes:
        attempted = consumed + len(chunk)
        if attempted > declared:
            with pytest.raises(SandboxUploadSizeMismatchError) as overflow_info:
                await target.write(chunk)
            error = overflow_info.value
            assert (error.path, error.declared, error.consumed, error.early_end) == (
                "data.bin",
                declared,
                attempted,
                False,
            )
            assert upload.writes == forwarded
            break

        if inject_failure:
            write_error = RuntimeError("write failed")
            upload.write_error = write_error
            with pytest.raises(RuntimeError) as write_info:
                await target.write(chunk)
            assert write_info.value is write_error
            upload.write_error = None
            assert upload.writes == forwarded

        await target.write(chunk)
        forwarded.append(chunk)
        consumed = attempted

    assert upload.writes == forwarded
    if consumed == declared:
        await target.finish()
        assert upload.finishes == 1
    else:
        with pytest.raises(SandboxUploadSizeMismatchError) as underflow_info:
            await target.finish()
        error = underflow_info.value
        assert (error.path, error.declared, error.consumed, error.early_end) == (
            "data.bin",
            declared,
            consumed,
            True,
        )
        assert upload.finishes == 0


class _CoreTarget:
    def __init__(
        self,
        *,
        finish_error: BaseException | None = None,
        abort_error: BaseException | None = None,
    ) -> None:
        self.writes: list[bytes] = []
        self.flushes = 0
        self.finishes = 0
        self.aborts = 0
        self.releases = 0
        self.finish_error = finish_error
        self.abort_error = abort_error

    async def write(self, data: bytes) -> None:
        self.writes.append(data)

    async def flush(self) -> None:
        self.flushes += 1

    async def finish(self) -> None:
        self.finishes += 1
        if self.finish_error is not None:
            raise self.finish_error

    async def abort(self) -> None:
        self.aborts += 1
        if self.abort_error is not None:
            raise self.abort_error


class _CoreTargetSource:
    def __init__(self, name: str, target: _CoreTarget) -> None:
        self.name = name
        self._target = target

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[_WriteTarget]:
        try:
            yield self._target
        finally:
            self._target.releases += 1


def _target_source(target: _CoreTarget) -> _WriteTargetSource:
    return _CoreTargetSource("data.bin", target)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "expected_counts"),
    [
        ("success", (1, 1, 0, 1)),
        ("body_failure", (0, 0, 1, 1)),
        ("finish_failure", (0, 1, 1, 1)),
        ("abort_failure", (0, 0, 1, 1)),
    ],
)
async def test_writer_core_lifecycle(
    case: str,
    expected_counts: tuple[int, int, int, int],
) -> None:
    body_error = RuntimeError("body failed")
    finish_error = RuntimeError("finish failed") if case == "finish_failure" else None
    abort_error = RuntimeError("abort failed") if case == "abort_failure" else None
    target = _CoreTarget(finish_error=finish_error, abort_error=abort_error)
    core = BinaryWriterCore(_target_source(target))

    if case == "success":
        async with core.lifecycle():
            assert await core.write(bytearray(b"data")) == 4
            await core.flush()
            await core.close()
        assert target.writes == [b"data"]
    else:
        with pytest.raises(RuntimeError) as exc_info:
            async with core.lifecycle():
                if case in ("body_failure", "abort_failure"):
                    raise body_error
        assert exc_info.value is (finish_error or body_error)

    assert core.closed
    assert (target.flushes, target.finishes, target.aborts, target.releases) == expected_counts
