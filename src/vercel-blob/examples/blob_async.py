"""Upload bytes and adapted sync sources, download content, and delete the objects."""

from __future__ import annotations

import os
import tempfile
from collections.abc import AsyncIterator, Iterator
from uuid import uuid4

import anyio

from vercel import blob
from vercel.blob import (
    BlobPrefix,
    ListBlob,
    copy_blob,
    delete_blob,
    put_blob,
    query_blobs,
    query_entries,
)


async def iterate_in_thread(chunks: Iterator[bytes]) -> AsyncIterator[bytes]:
    """Yield chunks from a blocking iterator without blocking the event loop."""
    while (chunk := await anyio.to_thread.run_sync(next, chunks, None)) is not None:
        yield chunk


async def walk_blobs(prefix: str) -> AsyncIterator[ListBlob]:
    """Walk every directory level, yielding objects rather than virtual folders."""
    async for entry in query_entries(prefix=prefix, page_size=2):
        if isinstance(entry, BlobPrefix):
            # A returned prefix includes its trailing slash and can be queried directly.
            async for item in walk_blobs(entry.prefix):
                yield item
        else:
            yield entry


async def collections_example(access: blob.Access) -> None:
    """List a report collection, browse its folders, and build a download manifest."""
    prefix = f"vercel-py-examples/{uuid4().hex}/collections/"
    report = prefix + "reports/2026/january.csv"
    archived_report = prefix + "archive/2026/january.csv"
    readme = prefix + "README.txt"
    csv_body = b"month,revenue\nJanuary,1200\n"
    readme_body = b"Monthly reports and their archived copies.\n"
    try:
        original = await put_blob(report, csv_body, access=access, content_type="text/csv")
        await put_blob(readme, readme_body, access=access, content_type="text/plain")
        copied = await copy_blob(
            original.url,
            archived_report,
            access=access,
            content_type="text/csv",
            if_match=original.etag,
        )
        assert (await blob.get_blob(copied.url, access=access)).body == csv_body

        # Flat listing includes all descendants. Pagination is automatic.
        print("All objects, including nested reports:")
        paths: set[str] = set()
        total_bytes = 0
        async for item in query_blobs(prefix=prefix, page_size=2):
            print(f"  {item.pathname.removeprefix(prefix)}: {item.size} bytes")
            paths.add(item.pathname)
            total_bytes += item.size
        print(f"Total: {len(paths)} objects, {total_bytes} bytes")
        assert paths == {report, archived_report, readme}
        assert total_bytes == 2 * len(csv_body) + len(readme_body)

        # Directory listing shows only immediate objects and virtual subfolders.
        print("Top-level directory:")
        directories: set[str] = set()
        immediate_objects: set[str] = set()
        async for entry in query_entries(prefix=prefix, page_size=2):
            if isinstance(entry, BlobPrefix):
                print(f"  Folder: {entry.prefix.removeprefix(prefix)}")
                directories.add(entry.prefix)
            else:
                print(f"  Object: {entry.pathname.removeprefix(prefix)} ({entry.size} bytes)")
                immediate_objects.add(entry.pathname)
        assert directories == {prefix + "reports/", prefix + "archive/"}
        assert immediate_objects == {readme}

        # Traverse returned prefixes recursively when the application needs folder
        # boundaries. For a flat inventory, query_blobs above takes fewer requests.
        print("CSV download manifest from recursive directory traversal:")
        csv_paths: set[str] = set()
        async for item in walk_blobs(prefix):
            if item.pathname.endswith(".csv"):
                print(f"  {item.pathname.removeprefix(prefix)} -> {item.download_url}")
                csv_paths.add(item.pathname)
        assert csv_paths == {report, archived_report}
    finally:
        # Clean up known paths even if setup or traversal failed partway through.
        await delete_blob([report, archived_report, readme])


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

    await collections_example(access)

    print("Async Blob lifecycle passed; test object deleted.")


if __name__ == "__main__":
    anyio.run(main)
