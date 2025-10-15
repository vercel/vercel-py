from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from typing import Any, Callable, Awaitable, Iterable, Iterator, AsyncIterator

from ..utils import UploadProgressEvent, compute_body_length, create_put_headers, create_put_options
from ..errors import BlobError
from .core import (
    _create_multipart_upload,
    _create_multipart_upload_async,
    _upload_part as _upload_part_sync,
    _upload_part_async,
    _complete_multipart_upload,
    _complete_multipart_upload_async,
)

DEFAULT_PART_SIZE = 8 * 1024 * 1024  # 8MB
MIN_PART_SIZE = 5 * 1024 * 1024  # 5 MiB minimum for most backends; last part may be smaller
MAX_CONCURRENCY = 6


def _validate_part_size(part_size: int) -> int:
    ps = int(part_size)
    if ps < MIN_PART_SIZE:
        raise BlobError(f"part_size must be at least {MIN_PART_SIZE} bytes (5 MiB)")
    return ps


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
    options: dict[str, Any] = {
        "access": access,
        "contentType": content_type,
        "addRandomSuffix": add_random_suffix,
        "allowOverwrite": overwrite,
        "cacheControlMaxAge": cache_control_max_age,
        "token": token,
    }
    opts = create_put_options(path=path, options=options)
    headers = create_put_headers(
        ["cacheControlMaxAge", "addRandomSuffix", "allowOverwrite", "contentType"], opts
    )

    part_size = _validate_part_size(part_size)

    create_resp = _create_multipart_upload(path, headers, token=opts.get("token"))
    upload_id = create_resp["uploadId"]
    key = create_resp["key"]

    total = compute_body_length(body)
    loaded_per_part: dict[int, int] = {}
    loaded_lock = threading.Lock()
    results: list[dict] = []

    def upload_one(part_number: int, content: bytes) -> dict:
        def progress(evt: UploadProgressEvent) -> None:
            with loaded_lock:
                loaded_per_part[part_number] = int(evt.loaded)
                if on_upload_progress:
                    loaded = sum(loaded_per_part.values())
                    pct = round((loaded / total) * 100, 2) if total else 0.0
                    on_upload_progress(
                        UploadProgressEvent(loaded=loaded, total=total, percentage=pct)
                    )

        resp = _upload_part_sync(
            upload_id=upload_id,
            key=key,
            path=path,
            headers=headers,
            token=opts.get("token"),
            part_number=part_number,
            body=content,
            on_upload_progress=progress,
        )
        return {"partNumber": part_number, "etag": resp["etag"]}

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as executor:
        inflight = set()
        part_no = 1
        for chunk in _iter_part_bytes(body, part_size):
            fut = executor.submit(upload_one, part_no, chunk)
            inflight.add(fut)
            part_no += 1
            if len(inflight) >= MAX_CONCURRENCY:
                done, inflight = wait(inflight, return_when=FIRST_COMPLETED)
                for d in done:
                    results.append(d.result())

        if inflight:
            done, _ = wait(inflight)
            for d in done:
                results.append(d.result())

    # Ensure parts are ordered by partNumber
    results.sort(key=lambda p: int(p["partNumber"]))

    if on_upload_progress:
        on_upload_progress(UploadProgressEvent(loaded=total, total=total, percentage=100.0))

    return _complete_multipart_upload(
        upload_id=upload_id,
        key=key,
        path=path,
        headers=headers,
        token=opts.get("token"),
        parts=results,
    )


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
    on_upload_progress: Callable[[UploadProgressEvent], None]
    | Callable[[UploadProgressEvent], Awaitable[None]]
    | None = None,
    part_size: int = DEFAULT_PART_SIZE,
) -> dict[str, Any]:
    options: dict[str, Any] = {
        "access": access,
        "contentType": content_type,
        "addRandomSuffix": add_random_suffix,
        "allowOverwrite": overwrite,
        "cacheControlMaxAge": cache_control_max_age,
        "token": token,
    }
    opts = create_put_options(path=path, options=options)
    headers = create_put_headers(
        ["cacheControlMaxAge", "addRandomSuffix", "allowOverwrite", "contentType"], opts
    )

    part_size = _validate_part_size(part_size)

    create_resp = await _create_multipart_upload_async(path, headers, token=opts.get("token"))
    upload_id = create_resp["uploadId"]
    key = create_resp["key"]

    total = compute_body_length(body)
    loaded_per_part: dict[int, int] = {}
    results: list[dict] = []

    def _make_progress(part_number: int):
        if on_upload_progress and asyncio.iscoroutinefunction(on_upload_progress):

            async def progress_async(evt: UploadProgressEvent):
                loaded_per_part[part_number] = int(evt.loaded)
                loaded = sum(loaded_per_part.values())
                pct = round((loaded / total) * 100, 2) if total else 0.0
                await on_upload_progress(
                    UploadProgressEvent(loaded=loaded, total=total, percentage=pct)
                )

            return progress_async
        else:

            def progress(evt: UploadProgressEvent) -> None:
                loaded_per_part[part_number] = int(evt.loaded)
                if on_upload_progress:
                    loaded = sum(loaded_per_part.values())
                    pct = round((loaded / total) * 100, 2) if total else 0.0
                    on_upload_progress(
                        UploadProgressEvent(loaded=loaded, total=total, percentage=pct)
                    )

            return progress

    async def upload_one(part_number: int, content: bytes) -> dict:
        resp = await _upload_part_async(
            upload_id=upload_id,
            key=key,
            path=path,
            headers=headers,
            part_number=part_number,
            body=content,
            on_upload_progress=_make_progress(part_number),
            token=opts.get("token"),
        )
        return {"partNumber": part_number, "etag": resp["etag"]}

    inflight: set[asyncio.Task] = set()
    part_no = 1
    async for chunk in _aiter_part_bytes(body, part_size):
        t = asyncio.create_task(upload_one(part_no, chunk))
        inflight.add(t)
        part_no += 1
        if len(inflight) >= MAX_CONCURRENCY:
            done, inflight = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
            for d in done:
                results.append(d.result())

    if inflight:
        done, _ = await asyncio.wait(inflight, return_when=asyncio.ALL_COMPLETED)
        for d in done:
            results.append(d.result())

    results.sort(key=lambda p: int(p["partNumber"]))

    if on_upload_progress:
        loaded = sum(loaded_per_part.values())
        pct = round((loaded / total) * 100, 2) if total else 100.0
        result = on_upload_progress(UploadProgressEvent(loaded=loaded, total=total, percentage=pct))
        if asyncio.iscoroutine(result):
            await result

    return await _complete_multipart_upload_async(
        upload_id=upload_id,
        key=key,
        path=path,
        headers=headers,
        token=opts.get("token"),
        parts=results,
    )
