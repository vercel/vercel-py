from __future__ import annotations

from collections.abc import AsyncIterator

import anyio
import pytest

from vercel.queue._internal.streams import AsyncStreamPayload

from .helpers import (
    one_chunk,
    queue_streams_anyio_module,
    run_with_anyio_backend,
)


@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
def test_async_stream_payload_read_preserves_unread_bytes(anyio_backend: str) -> None:
    async def check() -> None:
        payload = AsyncStreamPayload(one_chunk(b"abcdef"))

        assert await payload.read(2) == b"ab"
        assert await payload.read() == b"cdef"
        assert await payload.read() == b""

    run_with_anyio_backend(check, anyio_backend)


@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
def test_async_stream_payload_read_pulls_only_needed_chunk(anyio_backend: str) -> None:
    async def check() -> None:
        yielded = 0

        async def body() -> AsyncIterator[bytes]:
            nonlocal yielded
            for chunk in [b"abc", b"def"]:
                yielded += 1
                yield chunk

        payload = AsyncStreamPayload(body())

        assert await payload.read(2) == b"ab"
        assert yielded == 1
        assert await payload.read() == b"cdef"
        assert yielded == 2

    run_with_anyio_backend(check, anyio_backend)


@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
def test_async_stream_payload_reader_methods_share_cursor(anyio_backend: str) -> None:
    async def check() -> None:
        async def body() -> AsyncIterator[bytes]:
            yield b"first\nsec"
            yield b"ond--third"

        payload = AsyncStreamPayload(body())

        assert await payload.readline() == b"first\n"
        assert await payload.readuntil(b"--") == b"second--"
        assert await payload.readexactly(5) == b"third"
        assert await payload.read() == b""

    run_with_anyio_backend(check, anyio_backend)


@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
def test_async_stream_payload_readexactly_reports_partial(anyio_backend: str) -> None:
    async def check() -> None:
        payload = AsyncStreamPayload(one_chunk(b"abc"))

        with pytest.raises(queue_streams_anyio_module().IncompleteRead):
            await payload.readexactly(4)

    run_with_anyio_backend(check, anyio_backend)


@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
def test_async_stream_payload_readuntil_reports_partial(anyio_backend: str) -> None:
    async def check() -> None:
        payload = AsyncStreamPayload(one_chunk(b"abc"))

        with pytest.raises(queue_streams_anyio_module().IncompleteRead):
            await payload.readuntil(b"--")

    run_with_anyio_backend(check, anyio_backend)


@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
def test_async_stream_payload_readuntil_is_bounded(anyio_backend: str) -> None:
    async def check() -> None:
        payload = AsyncStreamPayload(one_chunk(b"x" * (64 * 1024)))

        with pytest.raises(queue_streams_anyio_module().DelimiterNotFound):
            await payload.readuntil(b"--")

    run_with_anyio_backend(check, anyio_backend)


@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
def test_async_stream_payload_iterates_unread_bytes(anyio_backend: str) -> None:
    async def check() -> None:
        async def body() -> AsyncIterator[bytes]:
            yield b"abc"
            yield b"def"

        payload = AsyncStreamPayload(body())

        assert await payload.read(2) == b"ab"
        assert [chunk async for chunk in payload] == [b"c", b"def"]

    run_with_anyio_backend(check, anyio_backend)


@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
def test_async_stream_payload_rejects_concurrent_reads(anyio_backend: str) -> None:
    async def check() -> None:
        started = anyio.Event()
        release = anyio.Event()

        async def body() -> AsyncIterator[bytes]:
            started.set()
            yield b"first"
            await release.wait()
            yield b"second"

        payload = AsyncStreamPayload(body())
        async with anyio.create_task_group() as task_group:
            result: bytes | None = None

            async def read_all() -> None:
                nonlocal result
                result = await payload.read()

            task_group.start_soon(read_all)
            await started.wait()

            with pytest.raises(queue_streams_anyio_module().BusyResourceError):
                await payload.read(1)

            release.set()
        assert result == b"firstsecond"

    run_with_anyio_backend(check, anyio_backend)


@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
def test_async_stream_payload_propagates_source_exceptions(anyio_backend: str) -> None:
    async def check() -> None:
        async def body() -> AsyncIterator[bytes]:
            yield b"abc"
            raise ValueError("boom")

        payload = AsyncStreamPayload(body())

        with pytest.raises(ValueError, match="boom"):
            await payload.read()

    run_with_anyio_backend(check, anyio_backend)


@pytest.mark.parametrize("anyio_backend", ["asyncio", "trio"])
def test_async_stream_payload_finalize_drains_and_closes_once(
    anyio_backend: str,
) -> None:
    async def check() -> None:
        consumed: list[bytes] = []
        closed = 0

        async def body() -> AsyncIterator[bytes]:
            for chunk in [b"a", b"b"]:
                consumed.append(chunk)
                yield chunk

        async def close() -> None:
            nonlocal closed
            closed += 1

        payload = AsyncStreamPayload(body(), on_close=close)

        await payload.afinalize()
        await payload.afinalize()

        assert consumed == [b"a", b"b"]
        assert closed == 1

    run_with_anyio_backend(check, anyio_backend)
