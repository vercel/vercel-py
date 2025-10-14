from __future__ import annotations

import asyncio
from typing import Any, Callable

from .._helpers import UploadProgressEvent
from .._put_helpers import create_put_headers, create_put_options
from .core import (
    _create_multipart_upload,
    _upload_part as raw_upload_part,
    _complete_multipart_upload,
)

DEFAULT_PART_SIZE = 8 * 1024 * 1024  # 8MB
MAX_CONCURRENCY = 6


async def uncontrolled_multipart_upload(
    pathname: str,
    body: Any,
    *,
    access: str = 'public',
    content_type: str | None = None,
    add_random_suffix: bool = False,
    allow_overwrite: bool = False,
    cache_control_max_age: int | None = None,
    token: str | None = None,
    on_upload_progress: Callable[[UploadProgressEvent], None] | None = None,
) -> dict[str, Any]:
    options: dict[str, Any] = {
        'access': access,
        'contentType': content_type,
        'addRandomSuffix': add_random_suffix,
        'allowOverwrite': allow_overwrite,
        'cacheControlMaxAge': cache_control_max_age,
        'token': token,
    }
    opts = await create_put_options(pathname=pathname, options=options)
    headers = create_put_headers(['cacheControlMaxAge', 'addRandomSuffix', 'allowOverwrite', 'contentType'], opts)

    create_resp = await _create_multipart_upload(pathname, headers, token=opts.get('token'))
    upload_id = create_resp["uploadId"]
    key = create_resp["key"]

    async def iter_parts() -> list[bytes]:
        chunks: list[bytes] = []
        if isinstance(body, (bytes, bytearray, memoryview)):
            data = bytes(body)
            for i in range(0, len(data), DEFAULT_PART_SIZE):
                chunks.append(data[i : i + DEFAULT_PART_SIZE])
        elif isinstance(body, str):
            data = body.encode("utf-8")
            for i in range(0, len(data), DEFAULT_PART_SIZE):
                chunks.append(data[i : i + DEFAULT_PART_SIZE])
        elif hasattr(body, "read"):
            while True:
                b = body.read(DEFAULT_PART_SIZE)
                if not b:
                    break
                if not isinstance(b, (bytes, bytearray, memoryview)):
                    b = bytes(b)
                chunks.append(bytes(b))
        else:
            buffer = bytearray()
            for ch in body:
                if not isinstance(ch, (bytes, bytearray, memoryview)):
                    ch = bytes(ch)
                buffer.extend(ch)
                if len(buffer) >= DEFAULT_PART_SIZE:
                    chunks.append(bytes(buffer[:DEFAULT_PART_SIZE]))
                    del buffer[:DEFAULT_PART_SIZE]
            if buffer:
                chunks.append(bytes(buffer))
        return chunks

    chunks = await iter_parts()
    total = sum(len(c) for c in chunks)
    loaded_per_part: dict[int, int] = {}

    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    results: list[dict] = []

    async def upload_one(idx: int, content: bytes) -> None:
        async with sem:
            part_number = idx + 1

            def progress(_: UploadProgressEvent) -> None:
                loaded_per_part[part_number] = len(content)
                if on_upload_progress:
                    loaded = sum(loaded_per_part.values())
                    pct = round((loaded / total) * 100, 2) if total else 0.0
                    on_upload_progress(
                        UploadProgressEvent(loaded=loaded, total=total, percentage=pct)
                    )

            resp = await raw_upload_part(
                upload_id=upload_id,
                key=key,
                pathname=pathname,
                headers=headers,
                token=opts.get('token'),
                part_number=part_number,
                body=content,
                on_upload_progress=progress,
            )
            results.append({"partNumber": part_number, "etag": resp["etag"]})

    await asyncio.gather(*[upload_one(i, c) for i, c in enumerate(chunks)])

    if on_upload_progress:
        on_upload_progress(
            UploadProgressEvent(loaded=total, total=total, percentage=100.0)
        )

    return await _complete_multipart_upload(
        upload_id=upload_id,
        key=key,
        pathname=pathname,
        headers=headers,
        token=opts.get('token'),
        parts=results,
    )
