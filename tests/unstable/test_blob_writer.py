import io
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, cast

import anyio
import pytest
from hypothesis import assume, given, settings, strategies as st

from vercel._internal.byte_stream import (
    AsyncByteStreamRuntime,
    StagingFileRuntime,
    SyncByteStreamRuntime,
)
from vercel._internal.iter_coroutine import iter_coroutine
from vercel._internal.unstable.blob.async_runtime import (
    AsyncBlobBinaryWriter,
    AsyncBlobTextWriter,
)
from vercel._internal.unstable.blob.errors import (
    BlobAlreadyExistsError,
    BlobNotFoundError,
    BlobPreconditionFailedError,
)
from vercel._internal.unstable.blob.models import (
    BlobRangeResponse,
    BlobStatResult,
    MultipartPartState,
    MultipartUploadState,
    _parse_file_mode,
)
from vercel._internal.unstable.blob.options import BlobServiceOptions
from vercel._internal.unstable.blob.service import BlobService
from vercel._internal.unstable.blob.sync_runtime import (
    SyncBlobBinaryWriter,
    SyncBlobTextWriter,
)

UTC = timezone.utc

BINARY_UPDATE_OPS = st.lists(
    st.one_of(
        st.tuples(st.just("read"), st.integers(min_value=-1, max_value=12)),
        st.tuples(
            st.just("seek"),
            st.integers(min_value=-8, max_value=24),
            st.sampled_from([0, 1, 2]),
        ),
        st.tuples(
            st.just("truncate"),
            st.one_of(st.none(), st.integers(min_value=0, max_value=24)),
        ),
        st.tuples(st.just("write"), st.binary(max_size=8)),
    ),
    min_size=1,
    max_size=20,
)


def _stat(pathname: str, body: bytes, etag: str = '"new"') -> BlobStatResult:
    return BlobStatResult(
        pathname=pathname,
        url=f"https://blob.test/{pathname}",
        download_url=f"https://blob.test/{pathname}?download=1",
        size=len(body),
        etag=etag,
        uploaded_at=datetime(2026, 1, 1, tzinfo=UTC),
        content_type="application/octet-stream",
        content_disposition="inline",
        cache_control="",
    )


class _StagingFile:
    def __init__(self) -> None:
        self.file = io.BytesIO()
        self.max_read = 0
        self.max_size = 0
        self.max_write = 0
        self.read_calls = 0
        self.closed = False
        self.fail: str | None = None
        self.fail_exception: BaseException | None = None

    def _check(self, operation: str) -> None:
        if self.closed:
            raise ValueError("I/O operation on closed staging file")
        if self.fail == operation:
            if self.fail_exception is not None:
                raise self.fail_exception
            raise OSError(f"staging {operation} failed")

    async def read(self, size: int = -1, /) -> bytes:
        self._check("read")
        self.read_calls += 1
        self.max_read = max(self.max_read, size)
        return self.file.read(size)

    async def readinto(self, buffer, /) -> int:
        self._check("read")
        return self.file.readinto(buffer)

    async def write(self, data: bytes, /) -> int:
        self._check("write")
        written = self.file.write(data)
        self.max_write = max(self.max_write, len(data))
        self.max_size = max(self.max_size, len(self.file.getbuffer()))
        return written

    async def flush(self) -> None:
        self._check("flush")

    async def tell(self) -> int:
        self._check("tell")
        return self.file.tell()

    async def seek(self, offset: int, whence: int = 0, /) -> int:
        self._check("seek")
        return self.file.seek(offset, whence)

    async def truncate(self, size: int | None = None, /) -> int:
        self._check("truncate")
        target = self.file.tell() if size is None else size
        position = self.file.tell()
        current = len(self.file.getbuffer())
        if target > current:
            self.file.seek(current)
            self.file.write(b"\0" * (target - current))
            self.file.seek(position)
            return target
        return self.file.truncate(target)


class _Runtime:
    def __init__(self) -> None:
        self.files: list[_StagingFile] = []
        self.active = 0
        self.acquisitions = 0
        self.exits = 0
        self.fail_enter = False
        self.fail_exit = False
        self.next_file_fail: str | None = None

    @asynccontextmanager
    async def temporary_file(self):
        self.acquisitions += 1
        if self.fail_enter:
            raise OSError("staging acquisition failed")
        file = _StagingFile()
        file.fail = self.next_file_fail
        self.files.append(file)
        self.active += 1
        try:
            yield file
        finally:
            self.active -= 1
            self.exits += 1
            file.closed = True
            if self.fail_exit:
                raise OSError("staging cleanup failed")


class _Chunks:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.done = False
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self) -> bytes:
        if self.done:
            raise StopAsyncIteration
        self.done = True
        return self.body

    async def aclose(self) -> None:
        self.closed = True


class _Api:
    def __init__(self, existing: bytes | None = None) -> None:
        self.existing = existing
        self.calls: list[tuple[Any, ...]] = []
        self.parts: list[bytes] = []
        self.part_bodies: dict[str, bytes] = {}
        self.fail: str | None = None
        self.etag = '"old"'
        self.source_read_sizes: list[int] = []
        self.range_responses: list[_Chunks] = []
        self.range_calls: list[tuple[str, str, int, int]] = []
        self.expected_access: str | None = None
        self.put_options: list[tuple[str, str | None, int | None]] = []
        self.create_options: list[tuple[str, str | None, int | None]] = []
        self.replace_after_publication: bytes | None = None
        self.mutation_etags: list[str] = []

    def replace_externally(self, body: bytes) -> None:
        self.existing = body
        self.etag = '"external"'

    async def _consume(self, source, size: int) -> bytes:
        chunks = []
        remaining = size
        while remaining:
            requested = min(3, remaining)
            self.source_read_sizes.append(requested)
            chunk = await source.read(requested)
            assert chunk
            chunks.append(chunk)
            remaining -= len(chunk)
        assert await source.read(1) == b""
        return b"".join(chunks)

    async def stat(self, pathname: str) -> BlobStatResult:
        self.calls.append(("stat", pathname))
        if self.existing is None:
            raise BlobNotFoundError()
        return _stat(pathname, self.existing, self.etag)

    async def _published_stat(self, pathname: str, published_etag: str) -> BlobStatResult:
        self.mutation_etags.append(published_etag)
        if self.replace_after_publication is not None:
            replacement = self.replace_after_publication
            self.replace_after_publication = None
            self.replace_externally(replacement)
        result = await self.stat(pathname)
        if result.etag != published_etag:
            raise BlobPreconditionFailedError(
                "publication succeeded but metadata provenance was lost"
            )
        return result

    async def read_range(self, stat, *, access, start: int, end: int):
        assert self.existing is not None
        if self.fail == "range":
            raise OSError("range failed")
        assert stat.etag == self.etag
        if self.expected_access is not None:
            assert access == self.expected_access
        assert 0 <= start <= end < len(self.existing)
        self.range_calls.append((stat.etag, access, start, end))
        body = self.existing[start : end + 1]
        chunks = _Chunks(body)
        self.range_responses.append(chunks)
        return BlobRangeResponse(cast(Any, chunks), start=start, end=end, total=len(self.existing))

    async def put(
        self,
        pathname,
        source,
        *,
        size,
        access,
        content_type,
        cache_control_max_age,
        exclusive,
        if_match=None,
    ):
        if self.fail == "put":
            raise RuntimeError("put failed")
        body = await self._consume(source, size)
        self.put_options.append((access, content_type, cache_control_max_age))
        self.calls.append(("put", pathname, body, exclusive, if_match))
        if exclusive and self.existing is not None:
            raise BlobAlreadyExistsError("already exists")
        if if_match is not None and if_match != self.etag:
            raise BlobPreconditionFailedError("precondition failed")
        self.existing = body
        self.etag = '"new"'
        return await self._published_stat(pathname, self.etag)

    async def create_multipart_upload(
        self, pathname, *, access, content_type, cache_control_max_age
    ):
        if self.fail == "create":
            raise RuntimeError("create failed")
        self.calls.append(("create", pathname))
        self.create_options.append((access, content_type, cache_control_max_age))
        return MultipartUploadState(pathname, "upload", "key")

    async def upload_part(self, upload, *, part_number, source, size):
        if self.fail == "part":
            raise RuntimeError("part failed")
        body = await self._consume(source, size)
        self.parts.append(body)
        self.calls.append(("part", part_number, body))
        etag = f'"{part_number}"'
        self.part_bodies[etag] = body
        return MultipartPartState(part_number, etag)

    async def complete_multipart_upload(self, upload, parts, *, exclusive, if_match):
        if self.fail == "complete":
            raise RuntimeError("complete failed")
        assert [part.part_number for part in parts] == list(range(1, len(parts) + 1))
        assert len({part.part_number for part in parts}) == len(parts)
        body = b"".join(self.part_bodies[part.etag] for part in parts)
        self.calls.append(("complete", exclusive, if_match))
        if exclusive and self.existing is not None:
            raise BlobAlreadyExistsError("already exists")
        if if_match is not None and if_match != self.etag:
            raise BlobPreconditionFailedError("precondition failed")
        self.existing = body
        self.etag = '"new"'
        return await self._published_stat(upload.pathname, self.etag)


def _service(
    api: _Api,
    runtime: Any,
    *,
    threshold: int = 8,
    part: int = 4,
    ensure_open=lambda: None,
):
    options = BlobServiceOptions()
    object.__setattr__(options, "multipart_threshold", threshold)
    object.__setattr__(options, "multipart_part_size", part)
    return BlobService(
        api_client=cast(Any, api),
        options=options,
        ensure_open=ensure_open,
        staging_file_runtime=cast(StagingFileRuntime, runtime),
    )


@pytest.mark.anyio
async def test_async_small_writer_publishes_only_on_close() -> None:
    api, runtime = _Api(), _Runtime()
    state = await _service(api, runtime).open_writer(
        "small", mode=_parse_file_mode("wb"), access="public"
    )
    writer = AsyncBlobBinaryWriter(state)

    assert writer.mode == "wb"
    assert await writer.write(b"hello") == 5
    assert writer.tell() == 5
    assert [call for call in api.calls if call[0] == "put"] == []
    await writer.flush()
    assert [call for call in api.calls if call[0] == "put"] == []
    await writer.close()

    assert ("put", "small", b"hello", False, None) in api.calls
    assert writer.stat.size == 5
    assert runtime.active == 0


@pytest.mark.anyio
async def test_async_threshold_crossing_uploads_ordered_bounded_parts() -> None:
    api, runtime = _Api(), _Runtime()
    state = await _service(api, runtime).open_writer(
        "large", mode=_parse_file_mode("wb"), access="public"
    )
    writer = AsyncBlobBinaryWriter(state)

    await writer.write(b"abcdefghij")
    assert api.parts == [b"abcd", b"efgh"]
    await writer.close()

    assert api.parts == [b"abcd", b"efgh", b"ij"]
    assert api.existing == b"abcdefghij"
    assert runtime.files[0].max_read <= 4
    assert runtime.files[0].max_size <= 8


@pytest.mark.anyio
async def test_mutation_materializes_and_uses_opened_etag() -> None:
    api, runtime = _Api(b"abcdef"), _Runtime()
    state = await _service(api, runtime).open_writer(
        "object", mode=_parse_file_mode("r+b"), access="private"
    )
    writer = AsyncBlobBinaryWriter(state)

    assert await writer.read(3) == b"abc"
    assert await writer.write(b"XY") == 2
    assert await writer.seek(0) == 0
    assert await writer.read() == b"abcXYf"
    await writer.close()

    assert ("put", "object", b"abcXYf", False, '"old"') in api.calls
    assert runtime.active == 0


@pytest.mark.anyio
async def test_append_forces_writes_to_eof_and_clean_close_is_noop() -> None:
    api, runtime = _Api(b"abc"), _Runtime()
    state = await _service(api, runtime).open_writer(
        "object", mode=_parse_file_mode("a+b"), access="public"
    )
    writer = AsyncBlobBinaryWriter(state)
    await writer.seek(0)
    assert await writer.write(b"d") == 1
    await writer.seek(0)
    assert await writer.read() == b"abcd"
    await writer.close()
    assert ("put", "object", b"abcd", False, '"old"') in api.calls

    clean_api, clean_runtime = _Api(b"abc"), _Runtime()
    clean = AsyncBlobBinaryWriter(
        await _service(clean_api, clean_runtime).open_writer(
            "object", mode=_parse_file_mode("a+b"), access="public"
        )
    )
    await clean.close()
    assert not any(call[0] == "put" for call in clean_api.calls)


@pytest.mark.anyio
async def test_exceptional_async_context_aborts_without_publication() -> None:
    api, runtime = _Api(), _Runtime()
    writer = AsyncBlobBinaryWriter(
        await _service(api, runtime).open_writer(
            "object", mode=_parse_file_mode("wb"), access="public"
        )
    )
    with pytest.raises(RuntimeError):
        async with writer:
            await writer.write(b"lost")
            raise RuntimeError("boom")
    assert not any(call[0] in ("put", "complete") for call in api.calls)
    assert runtime.active == 0
    with pytest.raises(ValueError):
        _ = writer.stat


def test_sync_binary_and_text_writers_are_stdlib_io() -> None:
    api, runtime = _Api(), _Runtime()
    binary = SyncBlobBinaryWriter(
        iter_coroutine(
            _service(api, runtime).open_writer(
                "binary", mode=_parse_file_mode("wb"), access="public"
            )
        )
    )
    assert isinstance(binary, io.BufferedIOBase)
    assert binary.write(b"abc") == 3
    binary.close()
    assert api.existing == b"abc"

    text_api, text_runtime = _Api(), _Runtime()
    text_binary = SyncBlobBinaryWriter(
        iter_coroutine(
            _service(text_api, text_runtime).open_writer(
                "text", mode=_parse_file_mode("w"), access="public"
            )
        )
    )
    text = SyncBlobTextWriter(text_binary, encoding="utf-8", errors="strict", newline="\r\n")
    assert isinstance(text, io.TextIOBase)
    assert text.write("a\nb") == 3
    text.close()
    assert text_api.existing == b"a\r\nb"


@pytest.mark.anyio
async def test_async_text_writer_encodes_incrementally() -> None:
    api, runtime = _Api(), _Runtime()
    binary = AsyncBlobBinaryWriter(
        await _service(api, runtime).open_writer(
            "text", mode=_parse_file_mode("w"), access="public"
        )
    )
    text = AsyncBlobTextWriter(binary, encoding="utf-16-le", errors="strict", newline=None)
    assert await text.write("a\nb") == 3
    await text.close()
    assert api.existing == "a\nb".encode("utf-16-le")


@pytest.mark.anyio
async def test_async_text_update_reads_seeks_and_writes_text() -> None:
    api, runtime = _Api(b"one\ntwo"), _Runtime()
    binary = AsyncBlobBinaryWriter(
        await _service(api, runtime).open_writer(
            "text", mode=_parse_file_mode("r+"), access="public"
        )
    )
    text = AsyncBlobTextWriter(binary)

    assert text.readable()
    assert text.seekable()
    assert await text.readline() == "one\n"
    assert await text.seek(0) == 0
    assert await text.write("X") == 1
    await text.close()

    assert api.existing == b"Xne\ntwo"


@pytest.mark.anyio
async def test_staged_binary_readline_reads_in_bounded_chunks() -> None:
    body = b"x" * (128 * 1024) + b"\nrest"
    api, runtime = _Api(body), _Runtime()
    writer = AsyncBlobBinaryWriter(
        await _service(api, runtime).open_writer(
            "object", mode=_parse_file_mode("r+b"), access="public"
        )
    )

    baseline = runtime.files[0].read_calls
    assert await writer.readline() == body[:-4]
    assert runtime.files[0].read_calls - baseline <= 3
    assert await writer.read() == b"rest"
    await writer.close()


@pytest.mark.anyio
async def test_staged_binary_readline_buffer_is_invalidated_by_mutations() -> None:
    api, runtime = _Api(b"abc\ndef\n"), _Runtime()
    writer = AsyncBlobBinaryWriter(
        await _service(api, runtime).open_writer(
            "object", mode=_parse_file_mode("r+b"), access="public"
        )
    )

    assert await writer.readline() == b"abc\n"
    await writer.seek(0)
    await writer.write(b"XY")
    await writer.seek(0)
    assert await writer.readline() == b"XYc\n"
    await writer.truncate(3)
    await writer.seek(0)
    assert await writer.readline() == b"XYc"
    await writer.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("newline", "expected"),
    [
        (None, ["a\n", "b\n", "c\n", "d"]),
        ("", ["a\r", "b\r\n", "c\n", "d"]),
        ("\n", ["a\rb\r\n", "c\n", "d"]),
        ("\r", ["a\r", "b\r", "\nc\nd"]),
        ("\r\n", ["a\rb\r\n", "c\nd"]),
    ],
)
async def test_async_staged_text_readline_newline_modes(
    newline: str | None, expected: list[str]
) -> None:
    api, runtime = _Api(b"a\rb\r\nc\nd"), _Runtime()
    text = AsyncBlobTextWriter(
        AsyncBlobBinaryWriter(
            await _service(api, runtime).open_writer(
                "text", mode=_parse_file_mode("r+"), access="public"
            )
        ),
        newline=newline,
    )

    assert [await text.readline() for _ in expected] == expected
    await text.close()


@pytest.mark.anyio
async def test_staged_text_readline_handles_crlf_split_across_chunks() -> None:
    line = "x" * (64 * 1024 - 1) + "\r\n"
    api, runtime = _Api((line + "tail").encode()), _Runtime()
    text = AsyncBlobTextWriter(
        AsyncBlobBinaryWriter(
            await _service(api, runtime).open_writer(
                "text", mode=_parse_file_mode("r+"), access="public"
            )
        ),
        newline="",
    )

    assert await text.readline() == line
    assert await text.readline() == "tail"
    assert runtime.files[0].read_calls <= 4
    await text.close()


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["r+b", "ab", "a+b", "a+"])
async def test_zero_length_mutation_write_remains_clean(mode: str) -> None:
    api, runtime = _Api(b"unchanged"), _Runtime()
    state = await _service(api, runtime).open_writer(
        "object", mode=_parse_file_mode(mode), access="public"
    )
    if mode.endswith("b"):
        writer: Any = AsyncBlobBinaryWriter(state)
        assert await writer.write(b"") == 0
    else:
        writer = AsyncBlobTextWriter(AsyncBlobBinaryWriter(state))
        assert await writer.write("") == 0
    await writer.close()

    assert api.existing == b"unchanged"
    assert not any(call[0] in ("put", "create", "complete") for call in api.calls)


def _reference_utf16_write_seek_write() -> bytes:
    target = io.BytesIO()
    wrapper = io.TextIOWrapper(target, encoding="utf-16")
    wrapper.write("A")
    wrapper.seek(0)
    wrapper.write("B")
    wrapper.flush()
    result = target.getvalue()
    wrapper.detach()
    return result


@pytest.mark.anyio
async def test_utf16_write_seek_write_restores_encoder_state() -> None:
    api, runtime = _Api(), _Runtime()
    text = AsyncBlobTextWriter(
        AsyncBlobBinaryWriter(
            await _service(api, runtime).open_writer(
                "text", mode=_parse_file_mode("w+"), access="public"
            )
        ),
        encoding="utf-16",
    )
    assert await text.write("A") == 1
    assert await text.seek(0) == 0
    assert await text.write("B") == 1
    await text.close()

    assert api.existing == _reference_utf16_write_seek_write()


@pytest.mark.anyio
async def test_utf16_append_does_not_insert_second_bom() -> None:
    original = "A".encode("utf-16")
    api, runtime = _Api(original), _Runtime()
    text = AsyncBlobTextWriter(
        AsyncBlobBinaryWriter(
            await _service(api, runtime).open_writer(
                "text", mode=_parse_file_mode("a+"), access="public"
            )
        ),
        encoding="utf-16",
    )
    await text.seek(0)
    await text.write("B")
    await text.close()

    assert api.existing == "AB".encode("utf-16")


@pytest.mark.anyio
async def test_empty_utf16_text_close_does_not_emit_bom() -> None:
    api, runtime = _Api(), _Runtime()
    text = AsyncBlobTextWriter(
        AsyncBlobBinaryWriter(
            await _service(api, runtime).open_writer(
                "text", mode=_parse_file_mode("w"), access="public"
            )
        ),
        encoding="utf-16",
    )
    await text.flush()
    await text.close()
    assert api.existing == b""


@pytest.mark.anyio
async def test_empty_and_missing_append_publish_empty_objects() -> None:
    for mode, exclusive in (("wb", False), ("a+b", True), ("w+b", False), ("x+b", True)):
        api, runtime = _Api(), _Runtime()
        writer = AsyncBlobBinaryWriter(
            await _service(api, runtime).open_writer(
                "empty", mode=_parse_file_mode(mode), access="public"
            )
        )
        await writer.close()
        assert ("put", "empty", b"", exclusive, None) in api.calls


@pytest.mark.anyio
async def test_sequential_writer_rejects_random_access_and_closed_writes() -> None:
    api, runtime = _Api(), _Runtime()
    writer = AsyncBlobBinaryWriter(
        await _service(api, runtime).open_writer(
            "object", mode=_parse_file_mode("wb"), access="public"
        )
    )
    with pytest.raises(io.UnsupportedOperation):
        await writer.read()
    with pytest.raises(io.UnsupportedOperation):
        await writer.seek(0)
    with pytest.raises(io.UnsupportedOperation):
        await writer.truncate()
    with pytest.raises(TypeError):
        await writer.write("text")  # type: ignore[arg-type]
    await writer.close()
    await writer.close()
    with pytest.raises(ValueError, match="closed"):
        await writer.write(b"x")


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("body", "multipart"),
    [(b"1234567", False), (b"12345678", True), (b"123456789", True)],
)
async def test_threshold_and_exact_part_boundaries(body: bytes, multipart: bool) -> None:
    api, runtime = _Api(), _Runtime()
    writer = AsyncBlobBinaryWriter(
        await _service(api, runtime, threshold=8, part=4).open_writer(
            "object", mode=_parse_file_mode("wb"), access="public"
        )
    )
    await writer.write(body)
    await writer.close()

    assert api.existing == body
    assert any(call[0] == "create" for call in api.calls) is multipart
    assert any(call[0] == "put" for call in api.calls) is (not multipart)
    if multipart:
        assert [len(part) for part in api.parts] == ([4, 4] if len(body) == 8 else [4, 4, 1])


@pytest.mark.anyio
async def test_unaligned_threshold_compacts_tail_and_preserves_order() -> None:
    api, runtime = _Api(), _Runtime()
    writer = AsyncBlobBinaryWriter(
        await _service(api, runtime, threshold=7, part=4).open_writer(
            "object", mode=_parse_file_mode("wb"), access="public"
        )
    )
    for chunk in (b"ab", b"cdefg", b"h", b"ijk"):
        await writer.write(chunk)
    await writer.flush()
    assert api.existing is None
    assert not any(call[0] in ("put", "complete") for call in api.calls)
    await writer.close()

    assert api.parts == [b"abcd", b"efgh", b"ijk"]
    assert api.existing == b"abcdefghijk"
    assert runtime.files[0].max_size <= 7
    assert runtime.files[0].max_write <= 7
    assert max(api.source_read_sizes) <= 3


@pytest.mark.anyio
async def test_multipart_transition_never_falls_back_to_put() -> None:
    api, runtime = _Api(), _Runtime()
    writer = AsyncBlobBinaryWriter(
        await _service(api, runtime).open_writer(
            "object", mode=_parse_file_mode("wb"), access="public"
        )
    )
    await writer.write(b"12345678")
    await writer.flush()
    await writer.close()
    assert not any(call[0] == "put" for call in api.calls)
    assert [call[0] for call in api.calls].count("complete") == 1


@pytest.mark.anyio
async def test_large_single_write_is_incrementally_staged() -> None:
    api, runtime = _Api(), _Runtime()
    writer = AsyncBlobBinaryWriter(
        await _service(api, runtime, threshold=32, part=16).open_writer(
            "object", mode=_parse_file_mode("wb"), access="public"
        )
    )
    body = bytes(range(256)) * 1024
    await writer.write(body)
    await writer.close()
    assert api.existing == body
    assert runtime.files[0].max_size <= 32
    assert runtime.files[0].max_write <= 32
    assert runtime.files[0].max_read <= 16


@pytest.mark.anyio
async def test_successful_async_and_sync_contexts_publish() -> None:
    async_api, async_runtime = _Api(), _Runtime()
    async with AsyncBlobBinaryWriter(
        await _service(async_api, async_runtime).open_writer(
            "async", mode=_parse_file_mode("wb"), access="public"
        )
    ) as writer:
        await writer.write(b"async")
    assert async_api.existing == b"async"

    sync_api, sync_runtime = _Api(), _Runtime()
    with SyncBlobBinaryWriter(
        iter_coroutine(
            _service(sync_api, sync_runtime).open_writer(
                "sync", mode=_parse_file_mode("wb"), access="public"
            )
        )
    ) as writer:
        writer.write(b"sync")
    assert sync_api.existing == b"sync"


@pytest.mark.anyio
async def test_w_replaces_existing_and_x_preserves_existing() -> None:
    replace_api, runtime = _Api(b"old"), _Runtime()
    replace = AsyncBlobBinaryWriter(
        await _service(replace_api, runtime).open_writer(
            "object", mode=_parse_file_mode("wb"), access="public"
        )
    )
    await replace.write(b"new")
    await replace.close()
    assert replace_api.existing == b"new"
    assert replace_api.calls[0][0] == "put"

    exclusive_api, exclusive_runtime = _Api(b"old"), _Runtime()
    exclusive = AsyncBlobBinaryWriter(
        await _service(exclusive_api, exclusive_runtime).open_writer(
            "object", mode=_parse_file_mode("xb"), access="public"
        )
    )
    await exclusive.write(b"new")
    with pytest.raises(BlobAlreadyExistsError):
        await exclusive.close()
    assert exclusive_api.existing == b"old"
    assert exclusive_runtime.active == 0


@pytest.mark.anyio
async def test_rplus_complete_binary_contract_and_clean_close() -> None:
    api, runtime = _Api(b"a\nbcdef"), _Runtime()
    api.expected_access = "private"
    writer = AsyncBlobBinaryWriter(
        await _service(api, runtime).open_writer(
            "object", mode=_parse_file_mode("r+b"), access="private"
        )
    )
    assert await writer.readline() == b"a\n"
    target = bytearray(2)
    assert await writer.readinto(target) == 2
    assert target == b"bc"
    assert await writer.seek(-2, io.SEEK_END) == 5
    assert await writer.read() == b"ef"
    assert await writer.seek(3) == 3
    assert await writer.truncate(5) == 5
    assert await writer.seek(3) == 3
    await writer.write(b"XY")
    await writer.close()
    assert api.existing == b"a\nbXY"
    assert ("put", "object", b"a\nbXY", False, '"old"') in api.calls
    assert all(response.closed for response in api.range_responses)
    assert api.range_calls == [('"old"', "private", 0, 6)]

    clean_api, clean_runtime = _Api(b"clean"), _Runtime()
    clean = AsyncBlobBinaryWriter(
        await _service(clean_api, clean_runtime).open_writer(
            "object", mode=_parse_file_mode("r+b"), access="public"
        )
    )
    await clean.close()
    assert not any(call[0] in ("put", "create", "complete") for call in clean_api.calls)


@settings(max_examples=75, deadline=None)
@given(initial=st.binary(max_size=20), operations=BINARY_UPDATE_OPS)
@pytest.mark.anyio
async def test_binary_update_mode_matches_bytes_io(
    initial: bytes, operations: list[tuple[Any, ...]]
) -> None:
    api, runtime = _Api(initial), _Runtime()
    writer = AsyncBlobBinaryWriter(
        await _service(api, runtime).open_writer(
            "object", mode=_parse_file_mode("r+b"), access="public"
        )
    )
    expected = io.BytesIO(initial)

    for operation in operations:
        name = operation[0]
        if name == "read":
            size = operation[1]
            try:
                read_result = expected.read(size)
            except Exception as exc:
                with pytest.raises(type(exc)):
                    await writer.read(size)
            else:
                assert await writer.read(size) == read_result
        elif name == "seek":
            _, offset, whence = operation
            base = {0: 0, 1: expected.tell(), 2: len(expected.getvalue())}[whence]
            assume(base + offset >= 0)
            try:
                seek_result = expected.seek(offset, whence)
            except Exception as exc:
                with pytest.raises(type(exc)):
                    await writer.seek(offset, whence)
            else:
                assert await writer.seek(offset, whence) == seek_result
        elif name == "truncate":
            size = operation[1]
            current_size = len(expected.getvalue())
            if size is None:
                assume(expected.tell() <= current_size)
            else:
                assume(size <= current_size)
            try:
                truncate_result = expected.truncate(size)
            except Exception as exc:
                with pytest.raises(type(exc)):
                    await writer.truncate(size)
            else:
                assert await writer.truncate(size) == truncate_result
        else:
            data = operation[1]
            try:
                write_result = expected.write(data)
            except Exception as exc:
                with pytest.raises(type(exc)):
                    await writer.write(data)
            else:
                assert await writer.write(data) == write_result

        assert writer.tell() == expected.tell()

    await writer.close()
    assert api.existing == expected.getvalue()


@pytest.mark.anyio
async def test_rplus_truncate_extension_zero_fills_like_tempfile() -> None:
    api, runtime = _Api(b"ab"), _Runtime()
    writer = AsyncBlobBinaryWriter(
        await _service(api, runtime).open_writer(
            "object", mode=_parse_file_mode("r+b"), access="public"
        )
    )
    assert await writer.truncate(5) == 5
    await writer.seek(0)
    assert await writer.read() == b"ab\0\0\0"
    await writer.close()
    assert api.existing == b"ab\0\0\0"


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["ab", "a+b"])
async def test_append_existing_and_missing_are_conditional(mode: str) -> None:
    existing_api, existing_runtime = _Api(b"abcd"), _Runtime()
    existing = AsyncBlobBinaryWriter(
        await _service(existing_api, existing_runtime).open_writer(
            "object", mode=_parse_file_mode(mode), access="public"
        )
    )
    if "+" in mode:
        await existing.seek(1)
        await existing.truncate(2)
    await existing.write(b"X")
    await existing.close()
    expected = b"abX" if "+" in mode else b"abcdX"
    assert existing_api.existing == expected
    assert any(call[-1] == '"old"' for call in existing_api.calls if call[0] == "put")

    missing_api, missing_runtime = _Api(), _Runtime()
    missing = AsyncBlobBinaryWriter(
        await _service(missing_api, missing_runtime).open_writer(
            "object", mode=_parse_file_mode(mode), access="public"
        )
    )
    await missing.write(b"X")
    await missing.close()
    assert ("put", "object", b"X", True, None) in missing_api.calls


@pytest.mark.anyio
async def test_wplus_truncates_and_xplus_is_create_only() -> None:
    truncate_api, runtime = _Api(b"old"), _Runtime()
    truncate = AsyncBlobBinaryWriter(
        await _service(truncate_api, runtime).open_writer(
            "object", mode=_parse_file_mode("w+b"), access="public"
        )
    )
    assert await truncate.read() == b""
    await truncate.close()
    assert truncate_api.existing == b""

    missing_api, missing_runtime = _Api(), _Runtime()
    missing = AsyncBlobBinaryWriter(
        await _service(missing_api, missing_runtime).open_writer(
            "object", mode=_parse_file_mode("x+b"), access="public"
        )
    )
    await missing.close()
    assert missing_api.existing == b""

    existing_api, existing_runtime = _Api(b"old"), _Runtime()
    existing = AsyncBlobBinaryWriter(
        await _service(existing_api, existing_runtime).open_writer(
            "object", mode=_parse_file_mode("x+b"), access="public"
        )
    )
    with pytest.raises(BlobAlreadyExistsError):
        await existing.close()
    assert existing_api.existing == b"old"


@pytest.mark.anyio
@pytest.mark.parametrize("body", [b"X", b"0123456789"])
async def test_etag_race_preserves_replacement_for_regular_and_multipart(body: bytes) -> None:
    api, runtime = _Api(b"old"), _Runtime()
    writer = AsyncBlobBinaryWriter(
        await _service(api, runtime).open_writer(
            "object", mode=_parse_file_mode("a+b"), access="public"
        )
    )
    await writer.write(body)
    api.replace_externally(b"replacement")
    with pytest.raises(BlobPreconditionFailedError) as raised:
        await writer.close()
    assert api.existing == b"replacement"
    assert runtime.active == 0
    with pytest.raises(BlobPreconditionFailedError) as again:
        await writer.close()
    assert again.value is raised.value


@pytest.mark.anyio
@pytest.mark.parametrize("body", [b"X", b"0123456789"])
async def test_postpublication_replacement_breaks_regular_and_multipart_writer(
    body: bytes,
) -> None:
    api, runtime = _Api(b"old"), _Runtime()
    api.replace_after_publication = b"replacement"
    writer = AsyncBlobBinaryWriter(
        await _service(api, runtime).open_writer(
            "object", mode=_parse_file_mode("wb"), access="public"
        )
    )
    await writer.write(body)

    with pytest.raises(BlobPreconditionFailedError, match="provenance") as raised:
        await writer.close()

    assert api.existing == b"replacement"
    assert runtime.active == 0
    with pytest.raises(ValueError):
        _ = writer.stat
    before = list(api.calls)
    with pytest.raises(BlobPreconditionFailedError) as again:
        await writer.close()
    assert again.value is raised.value
    assert api.calls == before


@pytest.mark.anyio
@pytest.mark.parametrize("body", [b"X", b"0123456789"])
async def test_matching_publication_stat_is_an_immutable_writer_snapshot(body: bytes) -> None:
    api, runtime = _Api(), _Runtime()
    writer = AsyncBlobBinaryWriter(
        await _service(api, runtime).open_writer(
            "object", mode=_parse_file_mode("wb"), access="public"
        )
    )
    await writer.write(body)
    await writer.close()
    published = writer.stat

    api.replace_externally(b"replacement")

    assert published.etag == '"new"'
    assert published.size == len(body)
    assert writer.stat is published
    assert api.mutation_etags == ['"new"']


@pytest.mark.anyio
async def test_mutation_multipart_publication_is_bounded_and_tail_safe() -> None:
    api, runtime = _Api(b"abc"), _Runtime()
    writer = AsyncBlobBinaryWriter(
        await _service(api, runtime, threshold=7, part=4).open_writer(
            "object", mode=_parse_file_mode("a+b"), access="private"
        )
    )
    await writer.write(b"defghijk")
    await writer.close()
    assert api.parts == [b"abcd", b"efgh", b"ijk"]
    assert api.existing == b"abcdefghijk"
    assert runtime.files[0].max_read <= 4
    assert all(response.closed for response in api.range_responses)


@pytest.mark.anyio
async def test_real_async_and_sync_staging_runtimes_publish() -> None:
    async_api = _Api()
    async_writer = AsyncBlobBinaryWriter(
        await _service(async_api, AsyncByteStreamRuntime()).open_writer(
            "async", mode=_parse_file_mode("wb"), access="public"
        )
    )
    await async_writer.write(b"async")
    await async_writer.close()
    assert async_api.existing == b"async"

    sync_api = _Api()
    sync_writer = SyncBlobBinaryWriter(
        iter_coroutine(
            _service(sync_api, SyncByteStreamRuntime()).open_writer(
                "sync", mode=_parse_file_mode("wb"), access="public"
            )
        )
    )
    sync_writer.write(b"sync")
    sync_writer.close()
    assert sync_api.existing == b"sync"


class _ConcurrentStagingFile(_StagingFile):
    def __init__(self) -> None:
        super().__init__()
        self.writing = False
        self.overlapped = False

    async def write(self, data: bytes, /) -> int:
        if self.writing:
            self.overlapped = True
        self.writing = True
        await anyio.sleep(0)
        try:
            return await super().write(data)
        finally:
            self.writing = False


class _ConcurrentRuntime(_Runtime):
    @asynccontextmanager
    async def temporary_file(self):
        self.acquisitions += 1
        file = _ConcurrentStagingFile()
        self.files.append(file)
        self.active += 1
        try:
            yield file
        finally:
            file.closed = True
            self.active -= 1
            self.exits += 1


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("newline", "expected"),
    [(None, b"a\nb"), ("", b"a\nb"), ("\n", b"a\nb"), ("\r", b"a\rb"), ("\r\n", b"a\r\nb")],
)
async def test_text_newline_modes(newline: str | None, expected: bytes) -> None:
    api, runtime = _Api(), _Runtime()
    text = AsyncBlobTextWriter(
        AsyncBlobBinaryWriter(
            await _service(api, runtime).open_writer(
                "text", mode=_parse_file_mode("w"), access="public"
            )
        ),
        newline=newline,
    )
    await text.write("a\nb")
    await text.close()
    assert api.existing == expected


@pytest.mark.anyio
async def test_text_types_errors_and_finalization() -> None:
    api, runtime = _Api(), _Runtime()
    binary = AsyncBlobBinaryWriter(
        await _service(api, runtime).open_writer(
            "text", mode=_parse_file_mode("w"), access="public"
        )
    )
    text = AsyncBlobTextWriter(binary, encoding="ascii", errors="replace")
    with pytest.raises(TypeError):
        await text.write(b"bytes")  # type: ignore[arg-type]
    assert await text.write("café") == 4
    await text.close()
    assert api.existing == b"caf?"

    final_api, final_runtime = _Api(), _Runtime()
    final = AsyncBlobTextWriter(
        AsyncBlobBinaryWriter(
            await _service(final_api, final_runtime).open_writer(
                "stateful", mode=_parse_file_mode("w"), access="public"
            )
        ),
        encoding="iso2022_jp",
    )
    await final.write("日")
    await final.flush()
    assert final_api.existing is None
    await final.close()
    await final.close()
    assert final_api.existing == "日".encode("iso2022_jp")
    assert sum(call[0] in ("put", "complete") for call in final_api.calls) == 1


@pytest.mark.anyio
async def test_async_utf16_rplus_reads_then_rewrites_from_zero() -> None:
    api, runtime = _Api("A".encode("utf-16")), _Runtime()
    text = AsyncBlobTextWriter(
        AsyncBlobBinaryWriter(
            await _service(api, runtime).open_writer(
                "text", mode=_parse_file_mode("r+"), access="public"
            )
        ),
        encoding="utf-16",
    )
    assert await text.read() == "A"
    assert await text.seek(0) == 0
    assert await text.write("B") == 1
    await text.close()
    assert api.existing == "B".encode("utf-16")


@pytest.mark.anyio
async def test_utf16_read_cookie_seek_then_write_preserves_encoder_state() -> None:
    api, runtime = _Api("AC".encode("utf-16")), _Runtime()
    text = AsyncBlobTextWriter(
        AsyncBlobBinaryWriter(
            await _service(api, runtime).open_writer(
                "text", mode=_parse_file_mode("r+"), access="public"
            )
        ),
        encoding="utf-16",
    )
    assert await text.read(1) == "A"
    cookie = text.tell()
    assert await text.read() == "C"
    assert await text.seek(cookie) == cookie
    assert await text.write("B") == 1
    await text.close()
    assert api.existing == "AB".encode("utf-16")


@pytest.mark.anyio
async def test_text_rejects_arbitrary_seek_and_write_after_read() -> None:
    api, runtime = _Api(b"abc"), _Runtime()
    text = AsyncBlobTextWriter(
        AsyncBlobBinaryWriter(
            await _service(api, runtime).open_writer(
                "text", mode=_parse_file_mode("r+"), access="public"
            )
        )
    )
    assert await text.read(1) == "a"
    assert await text.write("x") == 1
    with pytest.raises(io.UnsupportedOperation, match="arbitrary"):
        await text.seek(999)
    await text.seek(0)
    await text.write("x")
    await text.close()
    assert api.existing == b"xxc"


@pytest.mark.anyio
async def test_text_truncate_translates_cookie_to_binary_position() -> None:
    api, runtime = _Api(), _Runtime()
    text = AsyncBlobTextWriter(
        AsyncBlobBinaryWriter(
            await _service(api, runtime).open_writer(
                "text", mode=_parse_file_mode("w+"), access="public"
            )
        ),
        encoding="utf-16",
    )
    await text.write("ABC")
    cookie = text.tell()
    assert await text.truncate(cookie) == 8
    assert await text.truncate() == 8
    with pytest.raises(io.UnsupportedOperation, match="arbitrary"):
        await text.truncate(999)
    await text.close()
    assert api.existing == "ABC".encode("utf-16")


@pytest.mark.anyio
async def test_text_flush_drains_encoder_without_publishing_or_duplication() -> None:
    api, runtime = _Api(), _Runtime()
    text = AsyncBlobTextWriter(
        AsyncBlobBinaryWriter(
            await _service(api, runtime).open_writer(
                "text", mode=_parse_file_mode("w"), access="public"
            )
        )
    )

    class _FlushEncoder:
        def __init__(self) -> None:
            self.pending = b""
            self.final_calls = 0

        def encode(self, value: str, final: bool = False) -> bytes:
            if value:
                self.pending += value.encode()
                return b""
            if final:
                self.final_calls += 1
            result, self.pending = self.pending, b""
            return result

        def getstate(self) -> int:
            return 0

        def setstate(self, state: int) -> None:
            pass

    encoder = _FlushEncoder()
    cast(Any, text._writer_state)._encoder = encoder
    await text.write("payload")
    await text.flush()
    await text.flush()
    assert api.existing is None
    assert runtime.files[0].file.getvalue() == b"payload"
    await text.close()
    assert api.existing == b"payload"
    assert encoder.final_calls == 1


@pytest.mark.anyio
async def test_async_text_finalization_error_aborts_without_publication() -> None:
    api, runtime = _Api(b"visible"), _Runtime()
    runtime.fail_exit = True
    text = AsyncBlobTextWriter(
        AsyncBlobBinaryWriter(
            await _service(api, runtime).open_writer(
                "text", mode=_parse_file_mode("w"), access="public"
            )
        ),
        encoding="idna",
    )
    assert await text.write("\ud800") == 1

    with pytest.raises(UnicodeError) as first:
        await text.close()
    with pytest.raises(UnicodeError) as second:
        await text.close()

    assert second.value is first.value
    assert api.existing == b"visible"
    assert runtime.active == 0
    with pytest.raises(ValueError, match="not published"):
        _ = text.stat


@pytest.mark.anyio
async def test_text_final_byte_write_error_aborts_without_publication() -> None:
    api, runtime = _Api(b"visible"), _Runtime()
    text = AsyncBlobTextWriter(
        AsyncBlobBinaryWriter(
            await _service(api, runtime).open_writer(
                "text", mode=_parse_file_mode("w"), access="public"
            )
        ),
        encoding="iso2022_jp",
    )
    await text.write("日")
    failure = OSError("final staging write failed")
    runtime.files[0].fail = "write"
    runtime.files[0].fail_exception = failure

    with pytest.raises(OSError) as first:
        await text.close()
    with pytest.raises(OSError) as second:
        await text.close()

    assert first.value is failure
    assert second.value is failure
    assert api.existing == b"visible"
    assert runtime.active == 0
    with pytest.raises(ValueError, match="not published"):
        _ = text.stat


@pytest.mark.anyio
@pytest.mark.parametrize("encoding", ["utf-8", "utf-16"])
async def test_async_text_read_then_write_aligns_codec_state(encoding: str) -> None:
    api, runtime = _Api("ABC".encode(encoding)), _Runtime()
    text = AsyncBlobTextWriter(
        AsyncBlobBinaryWriter(
            await _service(api, runtime).open_writer(
                "text", mode=_parse_file_mode("r+"), access="public"
            )
        ),
        encoding=encoding,
    )
    assert await text.read(1) == "A"
    assert await text.write("X") == 1
    await text.seek(0)
    assert await text.read() == "AXC"
    await text.close()
    assert api.existing == _textiowrapper_read_write_reference(encoding)


def _textiowrapper_read_write_reference(encoding: str) -> bytes:
    target = io.BytesIO("ABC".encode(encoding))
    reference = io.TextIOWrapper(target, encoding=encoding)
    assert reference.read(1) == "A"
    reference.seek(reference.tell())
    reference.write("X")
    reference.flush()
    result = target.getvalue()
    reference.detach()
    return result


class _Py310Failure(RuntimeError):
    add_note = None  # type: ignore[assignment]


class _ThreadBlockingStagingFile(_StagingFile):
    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()
        self.block_once = True

    async def write(self, data: bytes, /) -> int:
        if self.block_once:
            self.block_once = False
            self.entered.set()
            assert self.release.wait(2)
        return await super().write(data)


class _ThreadBlockingRuntime(_Runtime):
    @asynccontextmanager
    async def temporary_file(self):
        self.acquisitions += 1
        file = _ThreadBlockingStagingFile()
        self.files.append(file)
        self.active += 1
        try:
            yield file
        finally:
            file.closed = True
            self.active -= 1
            self.exits += 1


class _AsyncBlockingStagingFile(_StagingFile):
    def __init__(self) -> None:
        super().__init__()
        self.write_started = anyio.Event()
        self.write_release = anyio.Event()

    async def write(self, data: bytes, /) -> int:
        self.write_started.set()
        await self.write_release.wait()
        return await super().write(data)


class _AsyncBlockingRuntime(_Runtime):
    @asynccontextmanager
    async def temporary_file(self):
        self.acquisitions += 1
        file = _AsyncBlockingStagingFile()
        self.files.append(file)
        self.active += 1
        try:
            yield file
        finally:
            file.closed = True
            self.active -= 1
            self.exits += 1


class _RetryableExitContext:
    def __init__(self, runtime: "_RetryableExitRuntime") -> None:
        self.runtime = runtime
        self.file = _StagingFile()

    async def __aenter__(self) -> _StagingFile:
        self.runtime.active += 1
        return self.file

    async def __aexit__(self, *args: object) -> None:
        self.runtime.exit_attempts += 1
        if self.runtime.exit_attempts == 1:
            self.runtime.exit_started.set()
            await anyio.sleep_forever()
        self.file.closed = True
        self.runtime.active -= 1
        self.runtime.exits += 1


class _RetryableExitRuntime:
    def __init__(self) -> None:
        self.active = 0
        self.exits = 0
        self.exit_attempts = 0
        self.exit_started = anyio.Event()

    def temporary_file(self) -> _RetryableExitContext:
        return _RetryableExitContext(self)


class _BlockingPutApi(_Api):
    def __init__(self) -> None:
        super().__init__()
        self.put_started = anyio.Event()
        self.put_calls = 0

    async def put(self, *args: Any, **kwargs: Any) -> BlobStatResult:
        self.put_calls += 1
        self.put_started.set()
        await anyio.sleep_forever()
        raise AssertionError("unreachable")


def _sync_writer(
    api: _Api,
    runtime: Any,
    mode: str,
    *,
    threshold: int = 8,
    part: int = 4,
    ensure_open=lambda: None,
) -> SyncBlobBinaryWriter:
    return SyncBlobBinaryWriter(
        iter_coroutine(
            _service(
                api,
                runtime,
                threshold=threshold,
                part=part,
                ensure_open=ensure_open,
            ).open_writer("object", mode=_parse_file_mode(mode), access="public")
        )
    )


@pytest.mark.anyio
async def test_service_validates_path_access_and_forwards_metadata() -> None:
    api, runtime = _Api(), _Runtime()
    service = _service(api, runtime)
    with pytest.raises(ValueError, match="trailing slash"):
        await service.open_writer("folder/", mode=_parse_file_mode("wb"), access="public")
    with pytest.raises(ValueError, match="access"):
        await service.open_writer(
            "object", mode=_parse_file_mode("wb"), access=cast(Any, "invalid")
        )
    assert runtime.acquisitions == 0

    writer = AsyncBlobBinaryWriter(
        await service.open_writer(
            "object",
            mode=_parse_file_mode("wb"),
            access="private",
            content_type="text/plain",
            cache_control_max_age=timedelta(seconds=60),
        )
    )
    await writer.write(b"meta")
    await writer.close()
    assert api.put_options == [("private", "text/plain", timedelta(seconds=60))]
