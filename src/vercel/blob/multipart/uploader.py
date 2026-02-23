from __future__ import annotations

import inspect
import threading
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Any, cast

import anyio

from ..._iter_coroutine import iter_coroutine
from ..errors import BlobError
from ..utils import (
    UploadProgressEvent,
    compute_body_length,
    create_put_headers,
    require_public_access,
)
from .core import _AsyncMultipartClient, _SyncMultipartClient

DEFAULT_PART_SIZE = 8 * 1024 * 1024  # 8MB
MIN_PART_SIZE = 5 * 1024 * 1024  # 5 MiB minimum for most backends; last part may be smaller
MAX_CONCURRENCY = 6

SyncProgressCallback = Callable[[UploadProgressEvent], None]
AsyncProgressCallback = (
    Callable[[UploadProgressEvent], None] | Callable[[UploadProgressEvent], Awaitable[None]]
)
SyncPartUploadFn = Callable[..., dict[str, Any]]
AsyncPartUploadFn = Callable[..., Awaitable[dict[str, Any]]]


@dataclass(frozen=True)
class _MultipartUploadSession:
    upload_id: str
    key: str
    path: str
    headers: dict[str, str]
    token: str | None


def _validate_part_size(part_size: int) -> int:
    ps = int(part_size)
    if ps < MIN_PART_SIZE:
        raise BlobError(f"part_size must be at least {MIN_PART_SIZE} bytes (5 MiB)")
    return ps


def _prepare_upload_headers(
    *,
    access: str,
    content_type: str | None,
    add_random_suffix: bool,
    overwrite: bool,
    cache_control_max_age: int | None,
) -> dict[str, str]:
    require_public_access(access)
    return cast(
        dict[str, str],
        create_put_headers(
            content_type=content_type,
            add_random_suffix=add_random_suffix,
            allow_overwrite=overwrite,
            cache_control_max_age=cache_control_max_age,
        ),
    )


def _normalize_part_upload_result(part_number: int, response: dict[str, Any]) -> dict[str, Any]:
    return {"partNumber": part_number, "etag": response["etag"]}


def _order_uploaded_parts(parts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered_parts = list(parts)
    ordered_parts.sort(key=lambda part: int(part["partNumber"]))
    return ordered_parts


def _shape_complete_upload_result(response: dict[str, Any]) -> dict[str, Any]:
    shaped = {
        "url": response["url"],
        "downloadUrl": response["downloadUrl"],
        "pathname": response["pathname"],
        "contentType": response["contentType"],
        "contentDisposition": response["contentDisposition"],
    }
    for key, value in response.items():
        if key not in shaped:
            shaped[key] = value
    return shaped


def _aggregate_progress_event(loaded: int, total: int) -> UploadProgressEvent:
    percentage = round((loaded / total) * 100, 2) if total else 0.0
    return UploadProgressEvent(loaded=loaded, total=total, percentage=percentage)


def _iter_part_bytes(body: Any, part_size: int) -> Iterator[bytes]:
    # bytes-like
    if isinstance(body, (bytes, bytearray, memoryview)):
        view = memoryview(body)
        offset = 0
        while offset < len(view):
            end = min(offset + part_size, len(view))
            yield bytes(view[offset:end])
            offset = end
        return
    # str
    if isinstance(body, str):
        data = body.encode("utf-8")
        view = memoryview(data)
        offset = 0
        while offset < len(view):
            end = min(offset + part_size, len(view))
            yield bytes(view[offset:end])
            offset = end
        return
    # file-like object
    if hasattr(body, "read"):
        while True:
            chunk = body.read(part_size)  # type: ignore[attr-defined]
            if not chunk:
                break
            if not isinstance(chunk, (bytes, bytearray, memoryview)):
                chunk = bytes(chunk)
            yield bytes(chunk)
        return
    # Iterable[bytes]
    if isinstance(body, Iterable):  # type: ignore[arg-type]
        buffer = bytearray()
        for ch in body:  # type: ignore[assignment]
            if not isinstance(ch, (bytes, bytearray, memoryview)):
                ch = bytes(ch)
            buffer.extend(ch)
            while len(buffer) >= part_size:
                yield bytes(buffer[:part_size])
                del buffer[:part_size]
        if buffer:
            yield bytes(buffer)
        return
    # Fallback: coerce to bytes and slice
    data = bytes(body)
    view = memoryview(data)
    offset = 0
    while offset < len(view):
        end = min(offset + part_size, len(view))
        yield bytes(view[offset:end])
        offset = end


async def _aiter_part_bytes(body: Any, part_size: int) -> AsyncIterator[bytes]:
    # AsyncIterable[bytes]
    if hasattr(body, "__aiter__"):
        buffer = bytearray()
        async for ch in body:  # type: ignore[misc]
            if not isinstance(ch, (bytes, bytearray, memoryview)):
                ch = bytes(ch)
            buffer.extend(ch)
            while len(buffer) >= part_size:
                yield bytes(buffer[:part_size])
                del buffer[:part_size]
        if buffer:
            yield bytes(buffer)
        return
    # Delegate to sync iterator for other cases
    for chunk in _iter_part_bytes(body, part_size):
        yield chunk


class _SyncMultipartUploadRuntime:
    def upload(
        self,
        *,
        session: _MultipartUploadSession,
        body: Any,
        part_size: int,
        total: int,
        on_upload_progress: SyncProgressCallback | None,
        upload_part_fn: SyncPartUploadFn,
    ) -> list[dict[str, Any]]:
        loaded_per_part: dict[int, int] = {}
        loaded_lock = threading.Lock()
        results: list[dict[str, Any]] = []

        def upload_one(part_number: int, content: bytes) -> dict[str, Any]:
            def progress(evt: UploadProgressEvent) -> None:
                with loaded_lock:
                    loaded_per_part[part_number] = int(evt.loaded)
                    if on_upload_progress:
                        loaded = sum(loaded_per_part.values())
                        on_upload_progress(_aggregate_progress_event(loaded=loaded, total=total))

            response = upload_part_fn(
                upload_id=session.upload_id,
                key=session.key,
                path=session.path,
                headers=session.headers,
                token=session.token,
                part_number=part_number,
                body=content,
                on_upload_progress=progress,
            )
            return _normalize_part_upload_result(part_number, response)

        with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as executor:
            inflight = set()
            part_number = 1
            for chunk in _iter_part_bytes(body, part_size):
                task = executor.submit(upload_one, part_number, chunk)
                inflight.add(task)
                part_number += 1
                if len(inflight) >= MAX_CONCURRENCY:
                    done, inflight = wait(inflight, return_when=FIRST_COMPLETED)
                    for completed in done:
                        results.append(completed.result())

            if inflight:
                done, _ = wait(inflight)
                for completed in done:
                    results.append(completed.result())

        if on_upload_progress:
            on_upload_progress(UploadProgressEvent(loaded=total, total=total, percentage=100.0))

        return results


class _AsyncMultipartUploadRuntime:
    async def upload(
        self,
        *,
        session: _MultipartUploadSession,
        body: Any,
        part_size: int,
        total: int,
        on_upload_progress: AsyncProgressCallback | None,
        upload_part_fn: AsyncPartUploadFn,
    ) -> list[dict[str, Any]]:
        loaded_per_part: dict[int, int] = {}
        results: list[dict[str, Any]] = []

        async def emit_progress(part_number: int, event: UploadProgressEvent) -> None:
            loaded_per_part[part_number] = int(event.loaded)
            if on_upload_progress:
                loaded = sum(loaded_per_part.values())
                callback_result = on_upload_progress(
                    _aggregate_progress_event(loaded=loaded, total=total)
                )
                if inspect.isawaitable(callback_result):
                    await cast(Awaitable[None], callback_result)

        def part_progress_callback(
            part_number: int,
        ) -> Callable[[UploadProgressEvent], Awaitable[None]]:
            async def callback(event: UploadProgressEvent) -> None:
                await emit_progress(part_number, event)

            return callback

        async def upload_one(part_number: int, content: bytes) -> dict[str, Any]:
            response = await upload_part_fn(
                upload_id=session.upload_id,
                key=session.key,
                path=session.path,
                headers=session.headers,
                part_number=part_number,
                body=content,
                on_upload_progress=part_progress_callback(part_number),
                token=session.token,
            )
            return _normalize_part_upload_result(part_number, response)

        semaphore = anyio.Semaphore(MAX_CONCURRENCY)
        results_by_part: dict[int, dict[str, Any]] = {}

        async def run_limited_upload(part_number: int, content: bytes) -> None:
            await semaphore.acquire()
            try:
                results_by_part[part_number] = await upload_one(part_number, content)
            finally:
                semaphore.release()

        part_number = 1
        async with anyio.create_task_group() as task_group:
            async for chunk in _aiter_part_bytes(body, part_size):
                task_group.start_soon(run_limited_upload, part_number, chunk)
                part_number += 1

        for ordered_part_number in sorted(results_by_part):
            results.append(results_by_part[ordered_part_number])

        if on_upload_progress:
            loaded = sum(loaded_per_part.values())
            percentage = round((loaded / total) * 100, 2) if total else 100.0
            callback_result = on_upload_progress(
                UploadProgressEvent(loaded=loaded, total=total, percentage=percentage)
            )
            if inspect.isawaitable(callback_result):
                await cast(Awaitable[None], callback_result)

        return results


def create_sync_multipart_upload_runtime() -> _SyncMultipartUploadRuntime:
    return _SyncMultipartUploadRuntime()


def create_async_multipart_upload_runtime() -> _AsyncMultipartUploadRuntime:
    return _AsyncMultipartUploadRuntime()


def auto_multipart_upload(
    path: str,
    body: Any,
    *,
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
    on_upload_progress: Callable[[UploadProgressEvent], None] | None = None,
    part_size: int = DEFAULT_PART_SIZE,
) -> dict[str, Any]:
    client = _SyncMultipartClient()
    headers = _prepare_upload_headers(
        access=access,
        content_type=content_type,
        add_random_suffix=add_random_suffix,
        overwrite=overwrite,
        cache_control_max_age=cache_control_max_age,
    )
    part_size = _validate_part_size(part_size)

    create_response = iter_coroutine(client.create_multipart_upload(path, headers, token=token))
    session = _MultipartUploadSession(
        upload_id=create_response["uploadId"],
        key=create_response["key"],
        path=path,
        headers=headers,
        token=token,
    )

    runtime = create_sync_multipart_upload_runtime()
    total = compute_body_length(body)
    parts = runtime.upload(
        session=session,
        body=body,
        part_size=part_size,
        total=total,
        on_upload_progress=on_upload_progress,
        upload_part_fn=lambda **kwargs: iter_coroutine(client.upload_part(**kwargs)),
    )
    ordered_parts = _order_uploaded_parts(parts)

    complete_response = iter_coroutine(
        client.complete_multipart_upload(
            upload_id=session.upload_id,
            key=session.key,
            path=session.path,
            headers=session.headers,
            token=session.token,
            parts=ordered_parts,
        )
    )
    return _shape_complete_upload_result(complete_response)


async def auto_multipart_upload_async(
    path: str,
    body: Any,
    *,
    access: str = "public",
    content_type: str | None = None,
    add_random_suffix: bool = False,
    overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
    on_upload_progress: (
        Callable[[UploadProgressEvent], None]
        | Callable[[UploadProgressEvent], Awaitable[None]]
        | None
    ) = None,
    part_size: int = DEFAULT_PART_SIZE,
) -> dict[str, Any]:
    client = _AsyncMultipartClient()
    headers = _prepare_upload_headers(
        access=access,
        content_type=content_type,
        add_random_suffix=add_random_suffix,
        overwrite=overwrite,
        cache_control_max_age=cache_control_max_age,
    )
    part_size = _validate_part_size(part_size)

    create_response = await client.create_multipart_upload(path, headers, token=token)
    session = _MultipartUploadSession(
        upload_id=create_response["uploadId"],
        key=create_response["key"],
        path=path,
        headers=headers,
        token=token,
    )

    runtime = create_async_multipart_upload_runtime()
    total = compute_body_length(body)
    parts = await runtime.upload(
        session=session,
        body=body,
        part_size=part_size,
        total=total,
        on_upload_progress=on_upload_progress,
        upload_part_fn=client.upload_part,
    )
    ordered_parts = _order_uploaded_parts(parts)

    complete_response = await client.complete_multipart_upload(
        upload_id=session.upload_id,
        key=session.key,
        path=session.path,
        headers=session.headers,
        token=session.token,
        parts=ordered_parts,
    )
    return _shape_complete_upload_result(complete_response)
