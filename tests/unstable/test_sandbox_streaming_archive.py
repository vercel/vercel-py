import gzip
import io
import random
import tarfile

import pytest

from vercel._internal.unstable.sandbox.errors import SandboxUploadSizeMismatchError
from vercel._internal.unstable.sandbox.runtime_common import (
    _UploadFileEntry,
    _validate_file_mode,
)
from vercel._internal.unstable.sandbox.streaming_archive import (
    _TarGzipEncoder,
    async_archive_body,
    sync_archive_body,
)


def _gunzip(data: bytes) -> bytes:
    return gzip.decompress(data)


def _collect_chunks(encoder: _TarGzipEncoder) -> bytes:
    return b"".join(encoder.finalize())


def _read_tar(data: bytes) -> list[tuple[str, bytes, int]]:
    decompressed = _gunzip(data)
    result: list[tuple[str, bytes, int]] = []
    with tarfile.open(fileobj=io.BytesIO(decompressed), mode="r:") as tar:
        for member in tar.getmembers():
            f = tar.extractfile(member)
            content = f.read() if f else b""
            result.append((member.name, content, member.mode))
    return result


class TestTarGzipEncoder:
    def test_empty_archive(self) -> None:
        encoder = _TarGzipEncoder(chunk_size=4096)
        data = _collect_chunks(encoder)
        decompressed = _gunzip(data)
        assert decompressed == b"\0" * 1024

    def test_single_empty_file(self) -> None:
        encoder = _TarGzipEncoder(chunk_size=4096)
        encoder.add_entry("empty.txt", 0)
        encoder.finish_entry()
        entries = _read_tar(_collect_chunks(encoder))
        assert len(entries) == 1
        assert entries[0] == ("empty.txt", b"", 0o644)

    def test_single_file_with_data(self) -> None:
        encoder = _TarGzipEncoder(chunk_size=4096)
        encoder.add_entry("hello.txt", 5)
        encoder.write_entry_data(b"hello")
        encoder.finish_entry()
        entries = _read_tar(_collect_chunks(encoder))
        assert len(entries) == 1
        assert entries[0] == ("hello.txt", b"hello", 0o644)

    def test_multiple_files(self) -> None:
        encoder = _TarGzipEncoder(chunk_size=4096)
        encoder.add_entry("a.txt", 3)
        encoder.write_entry_data(b"aaa")
        encoder.finish_entry()
        encoder.add_entry("b.txt", 3)
        encoder.write_entry_data(b"bbb")
        encoder.finish_entry()
        entries = _read_tar(_collect_chunks(encoder))
        assert len(entries) == 2
        assert entries[0] == ("a.txt", b"aaa", 0o644)
        assert entries[1] == ("b.txt", b"bbb", 0o644)

    def test_exact_512_byte_boundary(self) -> None:
        encoder = _TarGzipEncoder(chunk_size=4096)
        encoder.add_entry("first.txt", 512)
        encoder.write_entry_data(b"x" * 512)
        encoder.finish_entry()
        encoder.add_entry("second.txt", 1)
        encoder.write_entry_data(b"y")
        encoder.finish_entry()
        entries = _read_tar(_collect_chunks(encoder))
        assert len(entries) == 2
        assert entries[0] == ("first.txt", b"x" * 512, 0o644)
        assert entries[1] == ("second.txt", b"y", 0o644)

    def test_non_512_boundary(self) -> None:
        encoder = _TarGzipEncoder(chunk_size=4096)
        encoder.add_entry("data.txt", 100)
        encoder.write_entry_data(b"a" * 100)
        encoder.finish_entry()
        entries = _read_tar(_collect_chunks(encoder))
        assert len(entries) == 1
        assert entries[0] == ("data.txt", b"a" * 100, 0o644)

    def test_default_mode_644(self) -> None:
        encoder = _TarGzipEncoder(chunk_size=4096)
        encoder.add_entry("f", 0)
        encoder.finish_entry()
        entries = _read_tar(_collect_chunks(encoder))
        assert entries[0][2] == 0o644

    def test_explicit_mode(self) -> None:
        encoder = _TarGzipEncoder(chunk_size=4096)
        encoder.add_entry("f", 0, mode=0o755)
        encoder.finish_entry()
        entries = _read_tar(_collect_chunks(encoder))
        assert entries[0][2] == 0o755

    def test_explicit_zero_mode(self) -> None:
        encoder = _TarGzipEncoder(chunk_size=4096)
        encoder.add_entry("f", 0, mode=0)
        encoder.finish_entry()
        assert _read_tar(_collect_chunks(encoder))[0][2] == 0

    def test_unicode_path(self) -> None:
        encoder = _TarGzipEncoder(chunk_size=4096)
        encoder.add_entry("\N{SNOWMAN}.txt", 3)
        encoder.write_entry_data(b"abc")
        encoder.finish_entry()
        entries = _read_tar(_collect_chunks(encoder))
        assert entries[0][0] == "\N{SNOWMAN}.txt"

    def test_long_path(self) -> None:
        long_name = "a" * 200 + ".txt"
        encoder = _TarGzipEncoder(chunk_size=4096)
        encoder.add_entry(long_name, 1)
        encoder.write_entry_data(b"x")
        encoder.finish_entry()
        entries = _read_tar(_collect_chunks(encoder))
        assert entries[0][0] == long_name

    def test_absolute_path(self) -> None:
        encoder = _TarGzipEncoder(chunk_size=4096)
        encoder.add_entry("/etc/config", 1)
        encoder.write_entry_data(b"x")
        encoder.finish_entry()
        entries = _read_tar(_collect_chunks(encoder))
        assert entries[0][0] == "/etc/config"

    def test_short_source_raises(self) -> None:
        encoder = _TarGzipEncoder(chunk_size=4096)
        encoder.add_entry("f", 10)
        encoder.write_entry_data(b"12345")
        with pytest.raises(ValueError, match="Early end"):
            encoder.finish_entry()

    def test_trailing_source_data_raises(self) -> None:
        encoder = _TarGzipEncoder(chunk_size=4096)
        encoder.add_entry("f", 5)
        with pytest.raises(ValueError, match="Trailing data"):
            encoder.write_entry_data(b"123456")

    def test_finalize_exactly_once(self) -> None:
        encoder = _TarGzipEncoder(chunk_size=4096)
        list(encoder.finalize())
        with pytest.raises(RuntimeError, match="already finalized"):
            list(encoder.finalize())

    def test_finalize_closes_eagerly(self) -> None:
        encoder = _TarGzipEncoder(chunk_size=4096)
        chunks = encoder.finalize()
        with pytest.raises(RuntimeError, match="already finalized"):
            encoder.finalize()
        assert b"".join(chunks)

    @pytest.mark.parametrize("compressible", [True, False])
    def test_multi_megabyte_entry_is_bounded(self, compressible: bool) -> None:
        size = 3 * 1024 * 1024
        data = b"x" * size if compressible else random.Random(0).randbytes(size)
        encoder = _TarGzipEncoder(chunk_size=65536)
        encoder.add_entry("large.bin", size)
        chunks: list[bytes] = list(encoder.drain())
        for offset in range(0, size, 65536):
            encoder.write_entry_data(data[offset : offset + 65536])
            chunks.extend(encoder.drain())
            assert len(encoder._buffer) < 65536
        encoder.finish_entry()
        chunks.extend(encoder.drain())
        chunks.extend(encoder.finalize())
        assert _read_tar(b"".join(chunks))[0][1] == data

    def test_add_entry_after_finalize_raises(self) -> None:
        encoder = _TarGzipEncoder(chunk_size=4096)
        list(encoder.finalize())
        with pytest.raises(RuntimeError, match="already finalized"):
            encoder.add_entry("f", 0)

    def test_finish_entry_without_active_raises(self) -> None:
        encoder = _TarGzipEncoder(chunk_size=4096)
        with pytest.raises(RuntimeError, match="No active entry"):
            encoder.finish_entry()

    def test_write_without_active_raises(self) -> None:
        encoder = _TarGzipEncoder(chunk_size=4096)
        with pytest.raises(RuntimeError, match="No active entry"):
            encoder.write_entry_data(b"x")

    def test_add_entry_before_previous_finished_raises(self) -> None:
        encoder = _TarGzipEncoder(chunk_size=4096)
        encoder.add_entry("f", 1)
        with pytest.raises(RuntimeError, match="not finished"):
            encoder.add_entry("g", 0)

    def test_finalize_with_active_entry_raises(self) -> None:
        encoder = _TarGzipEncoder(chunk_size=4096)
        encoder.add_entry("f", 0)
        with pytest.raises(RuntimeError, match="active entry"):
            list(encoder.finalize())

    def test_next_chunk_returns_none_when_empty(self) -> None:
        encoder = _TarGzipEncoder(chunk_size=4096)
        assert encoder.next_chunk() is None

    def test_compressed_size_is_less_than_uncompressed_for_repeated_data(self) -> None:
        encoder = _TarGzipEncoder(chunk_size=4096)
        encoder.add_entry("f", 10000)
        encoder.write_entry_data(b"\0" * 10000)
        encoder.finish_entry()
        data = _collect_chunks(encoder)
        assert len(data) < 10000

    def test_data_smaller_than_chunk_size(self) -> None:
        encoder = _TarGzipEncoder(chunk_size=65536)
        encoder.add_entry("f", 100)
        encoder.write_entry_data(b"x" * 100)
        encoder.finish_entry()
        chunks = list(encoder.finalize())
        assert len(chunks) >= 1
        for chunk in chunks:
            assert len(chunk) > 0
            assert len(chunk) <= 65536


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

    def test_sync_single_file(self) -> None:
        entries = self._make_entries(("hello.txt", b"world", None))
        data = b"".join(sync_archive_body(entries, 4096))
        self._verify_entries(data, ("hello.txt", b"world", None))

    def test_sync_multiple_files(self) -> None:
        entries = self._make_entries(
            ("a.txt", b"aaa", 0o644),
            ("b.txt", b"bbbb", 0o755),
        )
        data = b"".join(sync_archive_body(entries, 4096))
        self._verify_entries(
            data,
            ("a.txt", b"aaa", 0o644),
            ("b.txt", b"bbbb", 0o755),
        )

    @pytest.mark.anyio
    async def test_async_single_file(self) -> None:
        entries = self._make_entries(("hello.txt", b"world", None))
        chunks: list[bytes] = []
        async for chunk in async_archive_body(entries, 4096):
            chunks.append(chunk)
        self._verify_entries(b"".join(chunks), ("hello.txt", b"world", None))

    @pytest.mark.anyio
    async def test_async_multiple_files(self) -> None:
        entries = self._make_entries(
            ("x.txt", b"data", 0o644),
            ("y.txt", b"more", 0o644),
        )
        chunks: list[bytes] = []
        async for chunk in async_archive_body(entries, 4096):
            chunks.append(chunk)
        self._verify_entries(
            b"".join(chunks),
            ("x.txt", b"data", 0o644),
            ("y.txt", b"more", 0o644),
        )

    @pytest.mark.anyio
    async def test_sync_and_async_produce_identical_bytes(self) -> None:
        entries = self._make_entries(
            ("a.txt", b"first", 0o755),
            ("b.txt", b"second", 0o644),
        )
        sync_data = b"".join(sync_archive_body(entries, 4096))
        async_chunks: list[bytes] = []
        async for chunk in async_archive_body(entries, 4096):
            async_chunks.append(chunk)
        assert sync_data == b"".join(async_chunks)

    def test_sync_trailing_data_raises(self) -> None:
        entry = _UploadFileEntry(path="f", size=3, source=b"extra")
        with pytest.raises(SandboxUploadSizeMismatchError) as exc_info:
            list(sync_archive_body([entry], 4096))
        assert (exc_info.value.path, exc_info.value.declared, exc_info.value.consumed) == (
            "f",
            3,
            4,
        )
        assert not exc_info.value.early_end

    @pytest.mark.anyio
    async def test_async_trailing_data_raises(self) -> None:
        entry = _UploadFileEntry(path="f", size=3, source=b"extra")
        with pytest.raises(SandboxUploadSizeMismatchError) as exc_info:
            async for _ in async_archive_body([entry], 4096):
                pass
        assert exc_info.value.consumed == 4
        assert not exc_info.value.early_end

    @pytest.mark.parametrize("compressible", [True, False])
    def test_sync_emits_before_source_is_consumed(self, compressible: bool) -> None:
        size = 2 * 1024 * 1024
        data = b"x" * size if compressible else random.Random(1).randbytes(size)

        class Reader:
            def __init__(self) -> None:
                self.offset = 0

            def read(self, size: int = -1, /) -> bytes:
                assert 0 < size <= 65536
                chunk = data[self.offset : self.offset + size]
                self.offset += len(chunk)
                return chunk

        reader = Reader()
        body = sync_archive_body([_UploadFileEntry("remote.bin", len(data), reader)], 65536)
        first = next(body)
        assert first
        assert reader.offset < len(data)
        assert _read_tar(first + b"".join(body))[0][1] == data

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

    def test_sync_oversized_read_counts_all_bytes(self) -> None:
        class Reader:
            def read(self, size: int = -1, /) -> bytes:
                return b"abcdef"

        with pytest.raises(SandboxUploadSizeMismatchError) as exc_info:
            list(sync_archive_body([_UploadFileEntry("f", 3, Reader())], 4))
        assert exc_info.value.consumed == 6

    def test_sync_non_bytes_and_source_errors_propagate(self) -> None:
        class BadReader:
            def read(self, size: int = -1, /) -> bytes:
                return bytearray(b"x")  # type: ignore[return-value]

        with pytest.raises(TypeError, match="non-bytes"):
            list(sync_archive_body([_UploadFileEntry("f", 1, BadReader())], 4))

        failure = RuntimeError("source failed")

        class FailingReader:
            def read(self, size: int = -1, /) -> bytes:
                raise failure

        with pytest.raises(RuntimeError) as exc_info:
            list(sync_archive_body([_UploadFileEntry("f", 1, FailingReader())], 4))
        assert exc_info.value is failure

    def test_sync_empty_archive(self) -> None:
        data = b"".join(sync_archive_body([], 4096))
        assert _gunzip(data) == b"\0" * 1024

    @pytest.mark.anyio
    async def test_async_empty_archive(self) -> None:
        chunks: list[bytes] = []
        async for chunk in async_archive_body([], 4096):
            chunks.append(chunk)
        assert _gunzip(b"".join(chunks)) == b"\0" * 1024


class TestNormalizeMode:
    """Tests for _normalize_mode."""

    def test_accepts_valid_mode(self) -> None:
        assert _validate_file_mode(0o644) == 0o644

    def test_accepts_none(self) -> None:
        assert _validate_file_mode(None) is None

    def test_rejects_bool(self) -> None:
        with pytest.raises(TypeError, match="mode must be an integer"):
            _validate_file_mode(True)

    def test_rejects_negative(self) -> None:
        with pytest.raises(ValueError, match="between 0 and 0o777"):
            _validate_file_mode(-1)

    def test_rejects_exceeds_0777(self) -> None:
        with pytest.raises(ValueError, match="between 0 and 0o777"):
            _validate_file_mode(0o1000)

    def test_rejects_float(self) -> None:
        with pytest.raises(TypeError, match="mode must be an integer"):
            _validate_file_mode(1.5)

    def test_rejects_string(self) -> None:
        with pytest.raises(TypeError, match="mode must be an integer"):
            _validate_file_mode("0o644")

    def test_encoder_rejects_invalid_mode(self) -> None:
        encoder = _TarGzipEncoder(4096)
        with pytest.raises(TypeError, match="mode must be an integer"):
            encoder.add_entry("test", 10, mode=True)

    def test_encoder_rejects_negative_mode(self) -> None:
        encoder = _TarGzipEncoder(4096)
        with pytest.raises(ValueError, match="between 0 and 0o777"):
            encoder.add_entry("test", 10, mode=-1)


class TestChunkSizeValidation:
    """Tests for chunk_size validation."""

    def test_rejects_zero_chunk_size(self) -> None:
        with pytest.raises(ValueError, match="chunk_size must be a positive integer"):
            _TarGzipEncoder(0)

    def test_rejects_negative_chunk_size(self) -> None:
        with pytest.raises(ValueError, match="chunk_size must be a positive integer"):
            _TarGzipEncoder(-1)

    def test_accepts_positive_chunk_size(self) -> None:
        encoder = _TarGzipEncoder(64)
        assert encoder._chunk_size == 64
