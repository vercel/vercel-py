import gzip
import io
import random
import tarfile

import pytest
from hypothesis import given, settings, strategies as st

from vercel.internal.core.byte_stream import AsyncByteStreamRuntime, SyncByteStreamRuntime
from vercel.internal.core.iter_coroutine import iter_coroutine
from vercel.sandbox._internal.errors import SandboxUploadSizeMismatchError
from vercel.sandbox._internal.runtime_common import _UploadFileEntry
from vercel.sandbox._internal.service import SandboxArchiveUpload
from vercel.sandbox._internal.streaming_archive import ArchiveRequestWriter


class _CollectRequest:
    def __init__(self) -> None:
        self.chunks: list[bytes] = []
        self.finishes = 0

    async def write(self, data: bytes) -> None:
        self.chunks.append(data)

    async def finish(self) -> None:
        self.finishes += 1

    async def abort(self) -> None:
        pass


def _sync_entries(entries: list[_UploadFileEntry]) -> list[_UploadFileEntry]:
    runtime = SyncByteStreamRuntime()
    return [
        _UploadFileEntry(
            path=entry.path,
            size=entry.size,
            source=runtime.reader(entry.source),
            mode=entry.mode,
            archive_path=entry.archive_path,
        )
        for entry in entries
    ]


def sync_archive_body(entries: list[_UploadFileEntry], chunk_size: int):  # type: ignore[no-untyped-def]
    request = _CollectRequest()

    async def upload_entries() -> None:
        upload = SandboxArchiveUpload(
            writer=ArchiveRequestWriter(request, chunk_size), paths=(), cwd="/"
        )
        for entry in _sync_entries(entries):
            await upload.add_source(entry)
        await upload.finish()

    iter_coroutine(upload_entries())
    return iter(request.chunks)


async def async_archive_body(entries: list[_UploadFileEntry], chunk_size: int):  # type: ignore[no-untyped-def]
    runtime = AsyncByteStreamRuntime()
    normalized = [
        _UploadFileEntry(
            path=entry.path,
            size=entry.size,
            source=runtime.reader(entry.source),
            mode=entry.mode,
            archive_path=entry.archive_path,
        )
        for entry in entries
    ]
    request = _CollectRequest()
    upload = SandboxArchiveUpload(
        writer=ArchiveRequestWriter(request, chunk_size), paths=(), cwd="/"
    )
    for entry in normalized:
        await upload.add_source(entry)
    await upload.finish()
    for chunk in request.chunks:
        yield chunk


def _read_tar(data: bytes) -> list[tuple[str, bytes, int]]:
    decompressed = gzip.decompress(data)
    result: list[tuple[str, bytes, int]] = []
    with tarfile.open(fileobj=io.BytesIO(decompressed), mode="r:") as tar:
        for member in tar.getmembers():
            f = tar.extractfile(member)
            content = f.read() if f else b""
            result.append((member.name, content, member.mode))
    return result


_PATH_SEGMENT = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-é雪",
    min_size=1,
    max_size=40,
)
_RELATIVE_PATH = st.lists(_PATH_SEGMENT, min_size=1, max_size=3).map("/".join)
_ARCHIVE_PATH = st.one_of(
    _RELATIVE_PATH,
    _RELATIVE_PATH.map(lambda path: f"/{path}"),
    st.text(alphabet="abcdefghijklmnopqrstuvwxyz", min_size=101, max_size=180).map(
        lambda path: f"{path}.bin"
    ),
)
_ARCHIVE_DATA = st.one_of(
    st.sampled_from([b"", b"x" * 511, b"x" * 512, b"x" * 513]),
    st.binary(max_size=8192),
)


class TestBodyIterators:
    @staticmethod
    def _make_entries(*specs: tuple[str, bytes, int | None]) -> list[_UploadFileEntry]:
        return [
            _UploadFileEntry(path=name, size=len(data), source=data, mode=mode)
            for name, data, mode in specs
        ]

    def _verify_entries(self, data: bytes, *specs: tuple[str, bytes, int | None]) -> None:
        entries = _read_tar(data)
        assert len(entries) == len(specs)
        for (name, content, mode), (exp_name, exp_data, exp_mode) in zip(
            entries, specs, strict=True
        ):
            assert name == exp_name
            assert content == exp_data
            assert mode == (exp_mode if exp_mode is not None else 0o644)

    @pytest.mark.anyio
    @settings(max_examples=50, deadline=None)
    @given(
        specs=st.lists(
            st.tuples(
                _ARCHIVE_PATH,
                _ARCHIVE_DATA,
                st.sampled_from([None, 0, 0o600, 0o644, 0o755, 0o777]),
            ),
            max_size=5,
            unique_by=lambda spec: spec[0],
        ),
        chunk_size=st.sampled_from([1, 64, 511, 512, 4096, 65536]),
    )
    async def test_sync_and_async_archive_parity(
        self,
        specs: list[tuple[str, bytes, int | None]],
        chunk_size: int,
    ) -> None:
        entries = self._make_entries(*specs)
        sync_data = b"".join(sync_archive_body(entries, chunk_size))
        async_chunks: list[bytes] = []
        async for chunk in async_archive_body(entries, chunk_size):
            async_chunks.append(chunk)
        assert sync_data == b"".join(async_chunks)
        self._verify_entries(sync_data, *specs)

    @pytest.mark.anyio
    async def test_async_trailing_data_raises(self) -> None:
        entry = _UploadFileEntry(path="f", size=3, source=b"extra")
        with pytest.raises(SandboxUploadSizeMismatchError) as exc_info:
            async for _ in async_archive_body([entry], 4096):
                pass
        assert exc_info.value.consumed == 4
        assert not exc_info.value.early_end

    def test_sync_emits_before_exhausting_source_with_bounded_reads(self) -> None:
        size = 2 * 1024 * 1024
        data = random.Random(1).randbytes(size)

        class Reader:
            def __init__(self) -> None:
                self.offset = 0
                self.max_requested = 0

            def read(self, size: int = -1, /) -> bytes:
                assert 0 < size <= 65536
                self.max_requested = max(self.max_requested, size)
                chunk = data[self.offset : self.offset + size]
                self.offset += len(chunk)
                return chunk

        reader = Reader()

        class ObservingRequest(_CollectRequest):
            def __init__(self) -> None:
                super().__init__()
                self.first_write_offset: int | None = None

            async def write(self, chunk: bytes) -> None:
                if self.first_write_offset is None:
                    self.first_write_offset = reader.offset
                await super().write(chunk)

        request = ObservingRequest()
        entries = _sync_entries([_UploadFileEntry("remote.bin", len(data), reader)])

        async def upload_entry() -> None:
            upload = SandboxArchiveUpload(
                writer=ArchiveRequestWriter(request, 65536), paths=(), cwd="/"
            )
            await upload.add_source(entries[0])
            await upload.finish()

        iter_coroutine(upload_entry())

        assert request.first_write_offset is not None
        assert request.first_write_offset < size
        assert 0 < reader.max_requested <= 65536
        assert _read_tar(b"".join(request.chunks))[0][1] == data

    def test_sync_early_end_error_fields(self) -> None:
        entry = _UploadFileEntry(path="visible/path", size=5, source=io.BytesIO(b"abc"))
        with pytest.raises(SandboxUploadSizeMismatchError) as exc_info:
            list(sync_archive_body([entry], 2))
        error = exc_info.value
        assert (error.path, error.declared, error.consumed, error.early_end) == (
            "visible/path",
            5,
            3,
            True,
        )

    def test_sync_non_bytes_and_source_errors_propagate(self) -> None:
        class BadReader:
            def read(self, size: int = -1, /) -> bytes:
                return bytearray(b"x")  # type: ignore[return-value]

        with pytest.raises(TypeError, match="expected bytes"):
            list(sync_archive_body([_UploadFileEntry("f", 1, BadReader())], 4))

        failure = RuntimeError("source failed")

        class FailingReader:
            def read(self, size: int = -1, /) -> bytes:
                raise failure

        with pytest.raises(RuntimeError) as exc_info:
            list(sync_archive_body([_UploadFileEntry("f", 1, FailingReader())], 4))
        assert exc_info.value is failure
