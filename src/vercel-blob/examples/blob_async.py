"""Upload bytes and adapted sync sources, download content, and delete the objects."""

from __future__ import annotations

import os
import tempfile
from collections.abc import AsyncIterator, Iterator
from uuid import uuid4

import anyio

from vercel import blob


async def iterate_in_thread(chunks: Iterator[bytes]) -> AsyncIterator[bytes]:
    """Yield chunks from a blocking iterator without blocking the event loop."""
    while (chunk := await anyio.to_thread.run_sync(next, chunks, None)) is not None:
        yield chunk


async def main() -> None:
    access: blob.Access = "private" if os.getenv("BLOB_TEST_ACCESS") == "private" else "public"
    pathname = f"vercel-py-examples/{uuid4().hex}/async.bin"
    payload = bytes(range(256)) * 1024
    uploaded = await blob.put_blob(
        pathname, payload, access=access, content_type="application/octet-stream"
    )
    try:
        result = await blob.get_blob(uploaded.url, access=access)
        assert result.body == payload
        assert result.metadata.content_type == uploaded.content_type
        received = bytearray()
        async with blob.stream_blob(uploaded.url, access=access) as download:
            async for chunk in download:
                received.extend(chunk)
        assert received == payload
        metadata = await blob.head_blob(uploaded.url)
        assert metadata.size == len(payload)
    finally:
        await blob.delete_blob(uploaded.url)

    # Adapt an open sync file with anyio.wrap_file; its reads run on a worker thread
    file_pathname = f"vercel-py-examples/{uuid4().hex}/async-file.bin"
    with tempfile.NamedTemporaryFile("w+b") as tmp:
        tmp.write(payload)
        tmp.flush()
        tmp.seek(0)
        file_uploaded = await blob.put_blob(
            file_pathname,
            anyio.wrap_file(tmp),
            access=access,
            content_length=len(payload),
            content_type="application/octet-stream",
        )
    try:
        assert (await blob.get_blob(file_uploaded.url, access=access)).body == payload
    finally:
        await blob.delete_blob(file_uploaded.url)

    # Adapt a sync iterator by pulling each chunk on a worker thread
    def sync_chunks() -> Iterator[bytes]:
        for offset in range(0, len(payload), 64 * 1024):
            yield payload[offset : offset + 64 * 1024]

    iter_pathname = f"vercel-py-examples/{uuid4().hex}/async-iter.bin"
    iter_uploaded = await blob.put_blob(
        iter_pathname,
        iterate_in_thread(sync_chunks()),
        access=access,
        content_length=len(payload),
        content_type="application/octet-stream",
    )
    try:
        assert (await blob.get_blob(iter_uploaded.url, access=access)).body == payload
    finally:
        await blob.delete_blob(iter_uploaded.url)

    # The caller chooses complete parts and their order. Successful exit publishes.
    multipart_pathname = f"vercel-py-examples/{uuid4().hex}/async-multipart.bin"

    async def part_chunks() -> AsyncIterator[bytes]:
        for _ in range(80):
            yield b"m" * (64 * 1024)

    async with blob.multipart_upload_blob(multipart_pathname, access=access) as upload:
        await upload.upload_part(part_chunks(), content_length=5 * 1024 * 1024)
        await upload.upload_part(b"tail")
    try:
        assert (await blob.get_blob(upload.result.url, access=access)).body == (
            b"m" * (5 * 1024 * 1024) + b"tail"
        )
    finally:
        await blob.delete_blob(upload.result.url)

    print("Async Blob lifecycle passed; test object deleted.")


if __name__ == "__main__":
    anyio.run(main)
