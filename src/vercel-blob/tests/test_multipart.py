"""Multipart publication and failure behavior through the public facades."""

from __future__ import annotations

import inspect
import io
import json
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from contextlib import AsyncExitStack, asynccontextmanager
from threading import Event, local
from typing import Any

import anyio
import httpx2 as httpx
import pytest
from test_object_ops import (
    TEST_TOKEN,
    _AsyncIncrementalReader,
    _AsyncStreamingMockTransport,
    _credentials,
    _IncrementalReader,
    _object_json,
    _StreamingMockTransport,
    _sync_session,
)

from vercel import blob
from vercel.api import session

_PART = b"a" * (5 * 1024 * 1024)


class Backend:
    def __init__(self, fail: str = "", malformed: str = "") -> None:
        self.actions: list[str] = []
        self.parts: list[bytes] = []
        self.fail = fail
        self.malformed = malformed
        self.source: Any = None
        self.first_read_count: int | None = None

    def respond(self, request: httpx.Request, body: bytes) -> httpx.Response:
        action = request.headers["x-mpu-action"]
        self.actions.append(action)
        assert request.method == "POST"
        assert request.url.path == "/api/blob/mpu"
        assert request.url.params["pathname"] == "café.bin"
        assert request.headers["authorization"] == f"Bearer {TEST_TOKEN}"
        assert request.headers["x-vercel-blob-access"] == "private"
        assert request.headers["x-allow-overwrite"] == "1"
        if action != "create":
            assert request.headers["x-mpu-key"] == "caf%C3%A9%20%2B%25.bin"
            assert request.headers["x-mpu-upload-id"] == "upload-id"
        if action == self.fail:
            return httpx.Response(403, json={"error": {"code": "forbidden"}})
        if action == self.malformed:
            return httpx.Response(200, json={})
        if action == "create":
            return httpx.Response(200, json={"key": "café +%.bin", "uploadId": "upload-id"})
        if action == "upload":
            number = len(self.parts) + 1
            assert request.headers["x-mpu-part-number"] == str(number)
            assert request.headers["content-length"] == str(len(body))
            self.parts.append(body)
            return httpx.Response(200, json={"etag": f'"etag-{number}"'})
        assert action == "complete"
        assert json.loads(body) == [
            {"partNumber": n, "etag": f'"etag-{n}"'} for n in range(1, len(self.parts) + 1)
        ]
        return httpx.Response(200, json=_object_json("café.bin"))


async def call(fn: Any, *args: Any, **kwargs: Any) -> Any:
    result = fn(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


@asynccontextmanager
async def scope(cm: Any, sync: bool) -> AsyncIterator[Any]:
    async with AsyncExitStack() as stack:
        yield stack.enter_context(cm) if sync else await stack.enter_async_context(cm)


@asynccontextmanager
async def runtime(sync: bool, backend: Backend) -> AsyncIterator[Any]:
    def handler(request: httpx.Request) -> httpx.Response:
        if backend.source is not None and request.headers["x-mpu-action"] == "upload":
            chunks = []
            assert isinstance(request.stream, httpx.SyncByteStream)
            for chunk in request.stream:
                if backend.first_read_count is None:
                    backend.first_read_count = backend.source.chunks_read
                chunks.append(chunk)
            return backend.respond(request, b"".join(chunks))
        return backend.respond(request, request.read())

    async def async_handler(request: httpx.Request) -> httpx.Response:
        if backend.source is not None and request.headers["x-mpu-action"] == "upload":
            chunks = []
            assert isinstance(request.stream, httpx.AsyncByteStream)
            async for chunk in request.stream:
                if backend.first_read_count is None:
                    backend.first_read_count = backend.source.chunks_read
                chunks.append(chunk)
            return backend.respond(request, b"".join(chunks))
        return backend.respond(request, await request.aread())

    options = (
        blob.sync.SyncBlobServiceOptions(credentials_factory=_credentials)
        if sync
        else blob.BlobServiceOptions(credentials_factory=_credentials)
    )
    factory: Any = (
        (lambda: httpx.Client(transport=_StreamingMockTransport(handler)))
        if sync
        else (lambda: httpx.AsyncClient(transport=_AsyncStreamingMockTransport(async_handler)))
    )
    async with scope(session(service_options=[options], httpx_client_factory=factory), sync):
        yield blob.sync if sync else blob


def upload(api: Any) -> Any:
    return api.multipart_upload_blob("café.bin", access="private", allow_overwrite=True)


@pytest.mark.anyio
@pytest.mark.parametrize("sync", [True, False])
async def test_multiple_streamed_parts_publish_in_order(sync: bool) -> None:
    backend = Backend()
    async with runtime(sync, backend) as api:
        cm = upload(api)
        with pytest.raises(blob.BlobError, match="only after successful"):
            _ = cm.result
        async with scope(cm, sync) as handle:
            reader = io.BytesIO(_PART)
            body = reader if sync else anyio.wrap_file(reader)
            await call(handle.upload_part, body, content_length=len(_PART))
            source = _IncrementalReader([b"tail"]) if sync else _AsyncIncrementalReader([b"tail"])
            await call(handle.upload_part, source, content_length=4)
            assert backend.actions == ["create", "upload", "upload"]
            assert not reader.closed
            with pytest.raises(blob.BlobError):
                _ = handle.result
        assert handle.result.pathname == "café.bin"
        assert backend.parts == [_PART, b"tail"]
        assert backend.actions == ["create", "upload", "upload", "complete"]
        with pytest.raises(blob.BlobError):
            await call(handle.upload_part, b"late")
        with pytest.raises(blob.BlobError):
            async with scope(cm, sync):
                pass
    assert handle.result.pathname == "café.bin"


@pytest.mark.anyio
@pytest.mark.parametrize("sync", [True, False])
@pytest.mark.parametrize(
    "case", ["empty", "exception", "caught", "short", "long", "zero", "small", "missing-length"]
)
async def test_failed_scope_never_completes(sync: bool, case: str) -> None:
    backend = Backend()
    async with runtime(sync, backend) as api:
        handle = upload(api)
        with pytest.raises((blob.BlobError, ValueError), match="boom|multipart|failed"):
            async with scope(handle, sync):
                if case == "empty":
                    pass
                elif case == "exception":
                    await call(handle.upload_part, b"prefix")
                    raise ValueError("boom")
                else:
                    await call(handle.upload_part, _PART)
                    if case == "caught":
                        backend.fail = "upload"
                    with pytest.raises((blob.BlobError, TypeError)):
                        if case in ("short", "long", "missing-length"):
                            body = io.BytesIO(b"abc")
                            source = body if sync else anyio.wrap_file(body)
                            await call(
                                handle.upload_part,
                                source,
                                content_length={"short": 4, "long": 2, "missing-length": None}[
                                    case
                                ],
                            )
                        elif case == "small":
                            await call(handle.upload_part, b"small")
                            await call(handle.upload_part, b"next")
                        else:
                            await call(handle.upload_part, b"" if case == "zero" else b"part")
                    if case in ("short", "long", "missing-length"):
                        assert not body.closed
                    with pytest.raises(blob.BlobError):
                        await call(handle.upload_part, b"cannot recover")
        assert "complete" not in backend.actions
        with pytest.raises(blob.BlobError):
            _ = handle.result


@pytest.mark.anyio
@pytest.mark.parametrize("sync", [True, False])
@pytest.mark.parametrize("action", ["create", "upload", "complete"])
@pytest.mark.parametrize("malformed", [False, True])
async def test_wire_failures_propagate(sync: bool, action: str, malformed: bool) -> None:
    backend = Backend(malformed=action) if malformed else Backend(fail=action)
    async with runtime(sync, backend) as api:
        handle = upload(api)
        with pytest.raises(blob.BlobStreamError if malformed else blob.BlobAccessError):
            async with scope(handle, sync):
                await call(handle.upload_part, b"part")
        assert backend.actions[-1] == action
        assert backend.actions.count(action) == 1
        with pytest.raises(blob.BlobError):
            _ = handle.result


@pytest.mark.anyio
async def test_cancelled_part_caught_inside_scope_prevents_publication() -> None:
    backend = Backend()
    async with runtime(False, backend) as api:
        handle = upload(api)
        with pytest.raises(blob.BlobError, match="failed"):
            async with handle:
                await handle.upload_part(_PART)

                async def source() -> AsyncIterator[bytes]:
                    await anyio.sleep_forever()
                    yield b"never"

                with anyio.move_on_after(0.01) as cancel:
                    await handle.upload_part(source(), content_length=10)
                assert cancel.cancel_called
        assert backend.actions == ["create", "upload"]


@pytest.mark.anyio
async def test_overlap_poisoning() -> None:
    backend = Backend()
    started = anyio.Event()
    release = anyio.Event()

    async def source() -> AsyncIterator[bytes]:
        started.set()
        await release.wait()
        yield b"part"

    async with runtime(False, backend) as api:
        handle = upload(api)
        with pytest.raises(blob.BlobError, match="failed"):
            async with handle:

                async def first() -> None:
                    with pytest.raises(blob.BlobError, match="failed"):
                        await handle.upload_part(source(), content_length=4)

                async with anyio.create_task_group() as tasks:
                    tasks.start_soon(first)
                    await started.wait()
                    with pytest.raises(blob.BlobError, match="overlapping"):
                        await handle.upload_part(b"second")
                    release.set()
        assert "complete" not in backend.actions


@pytest.mark.anyio
@pytest.mark.parametrize("sync", [True, False])
async def test_part_source_is_consumed_incrementally(sync: bool) -> None:
    backend = Backend()
    chunks = [b"x" * 1024] * 100
    backend.source = _IncrementalReader(chunks) if sync else _AsyncIncrementalReader(chunks)
    async with runtime(sync, backend) as api:
        async with scope(upload(api), sync) as handle:
            await call(handle.upload_part, backend.source, content_length=100 * 1024)
        assert backend.parts == [b"".join(chunks)]
        assert backend.first_read_count is not None
        assert 0 < backend.first_read_count < len(chunks)


def test_sync_multipart_without_event_loop() -> None:
    backend = Backend()

    def handler(request: httpx.Request) -> httpx.Response:
        return backend.respond(request, request.read())

    def early_return() -> blob.sync.SyncMultipartUpload:
        with upload(blob.sync) as handle:
            handle.upload_part(b"part")
            return handle

    with _sync_session(handler):
        handle = early_return()
    assert handle.result.pathname == "café.bin"
    assert backend.actions == ["create", "upload", "complete"]


@pytest.mark.anyio
@pytest.mark.parametrize("sync", [True, False])
async def test_oversize_part_rejected_before_reading(sync: bool) -> None:
    backend = Backend()

    class Reader(io.BytesIO):
        def read(self, size: int | None = -1) -> bytes:
            pytest.fail("invalid length must be rejected before reading")

    reader = Reader()
    async with runtime(sync, backend) as api:
        with pytest.raises(blob.BlobError, match="failed"):
            async with scope(upload(api), sync) as handle:
                with pytest.raises(blob.BlobError, match="5 GiB"):
                    await call(
                        handle.upload_part,
                        reader if sync else anyio.wrap_file(reader),
                        content_length=5 * 1024 * 1024 * 1024 + 1,
                    )
        assert backend.actions == ["create"]


@pytest.mark.anyio
async def test_pending_cancellation_on_exit_does_not_complete() -> None:
    backend = Backend()
    async with runtime(False, backend) as api:
        handle = upload(api)
        with anyio.CancelScope() as cancel:
            async with handle:
                await handle.upload_part(b"part")
                cancel.cancel()
        with pytest.raises(blob.BlobError):
            _ = handle.result
        assert backend.actions == ["create", "upload"]


@pytest.mark.anyio
@pytest.mark.parametrize("sync", [True, False])
async def test_invalid_creation_options_send_no_requests(sync: bool) -> None:
    backend = Backend()
    async with runtime(sync, backend) as api:
        with pytest.raises(blob.BlobError):
            api.multipart_upload_blob("", access="private")
        assert backend.actions == []


@pytest.mark.anyio
@pytest.mark.parametrize("sync", [True, False])
async def test_closed_session_does_not_complete(sync: bool) -> None:
    backend = Backend()
    async with runtime(sync, backend) as api:
        handle = upload(api)
        await call(handle.__enter__ if sync else handle.__aenter__)
        await call(handle.upload_part, b"part")
    from vercel.errors import VercelSessionClosedError

    with pytest.raises(VercelSessionClosedError):
        await call(handle.__exit__ if sync else handle.__aexit__, None, None, None)
    assert backend.actions == ["create", "upload"]


def test_sync_overlapping_threads_cannot_publish_a_prefix() -> None:
    backend = Backend()
    started = Event()
    release = Event()

    class Reader(io.BytesIO):
        def read(self, size: int | None = -1) -> bytes:
            started.set()
            assert release.wait(timeout=5)
            return super().read(size)

    def handler(request: httpx.Request) -> httpx.Response:
        return backend.respond(request, request.read())

    with _sync_session(handler):
        with pytest.raises(blob.BlobError, match="failed"):
            with upload(blob.sync) as handle:
                handle.upload_part(_PART)
                with ThreadPoolExecutor(max_workers=1) as pool:
                    first = pool.submit(handle.upload_part, Reader(b"part"), content_length=4)
                    try:
                        assert started.wait(timeout=5)
                        with pytest.raises(blob.BlobError, match="overlapping"):
                            handle.upload_part(b"second")
                    finally:
                        release.set()
                    with pytest.raises(blob.BlobError, match="failed"):
                        first.result(timeout=5)
    assert "complete" not in backend.actions


def test_sync_exit_while_overlap_failure_recording_is_paused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = Backend()
    source_started = Event()
    release_source = Event()
    overlap_detected = Event()
    release_rejected = Event()
    exiting = Event()
    thread_state = local()

    class Reader(io.BytesIO):
        def read(self, size: int | None = -1) -> bytes:
            source_started.set()
            assert release_source.wait(timeout=5)
            return super().read(size)

    def handler(request: httpx.Request) -> httpx.Response:
        return backend.respond(request, request.read())

    # Pause precisely between detecting overlap and recording the failure.
    # The completion decision must remain excluded throughout that interval.
    def pause_failure(instance: Any, name: str, value: Any) -> None:
        if name == "_failed" and value is True and getattr(thread_state, "rejecting", False):
            thread_state.rejecting = False
            overlap_detected.set()
            assert release_rejected.wait(timeout=5)
        object.__setattr__(instance, name, value)

    monkeypatch.setattr(blob.sync.SyncMultipartUpload, "__setattr__", pause_failure)
    with _sync_session(handler):
        handle = upload(blob.sync)
        handle.__enter__()
        handle.upload_part(_PART)

        def reject_part() -> None:
            thread_state.rejecting = True
            handle.upload_part(b"rejected")

        def exit_scope() -> None:
            exiting.set()
            handle.__exit__(None, None, None)

        with ThreadPoolExecutor(max_workers=3) as pool:
            active = pool.submit(handle.upload_part, Reader(b"tail"), content_length=4)
            try:
                assert source_started.wait(timeout=5)
                rejected = pool.submit(reject_part)
                assert overlap_detected.wait(timeout=5)
                release_source.set()
                # The original implementation lets the active part finish here.
                # With the fix it waits for the rejected call's state transition.
                try:
                    active.result(timeout=0.5)
                except FutureTimeoutError:
                    pass
                exit_result = pool.submit(exit_scope)
                assert exiting.wait(timeout=5)
                with pytest.raises(FutureTimeoutError):
                    exit_result.result(timeout=0.5)
                assert "complete" not in backend.actions
            finally:
                release_source.set()
                release_rejected.set()
            with pytest.raises(blob.BlobError, match="overlapping"):
                rejected.result(timeout=5)
            with pytest.raises(blob.BlobError, match="failed"):
                active.result(timeout=5)
            with pytest.raises(blob.BlobError, match="failed"):
                exit_result.result(timeout=5)
        with pytest.raises(blob.BlobError):
            _ = handle.result
    assert "complete" not in backend.actions
