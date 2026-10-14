"""Run the Blob object lifecycle without an event loop."""

from __future__ import annotations

import os
import tempfile
from collections.abc import Iterator
from uuid import uuid4

from vercel.blob import sync as blob
from vercel.blob.sync import (
    BlobPrefix,
    ListBlob,
    copy_blob,
    delete_blob,
    put_blob,
    query_blobs,
    query_entries,
)


def walk_blobs(prefix: str) -> Iterator[ListBlob]:
    """Walk every directory level, yielding objects rather than virtual folders."""
    for entry in query_entries(prefix=prefix, page_size=2):
        if isinstance(entry, BlobPrefix):
            # A returned prefix includes its trailing slash and can be queried directly.
            yield from walk_blobs(entry.prefix)
        else:
            yield entry


def collections_example(access: blob.Access) -> None:
    """List a report collection, browse its folders, and build a download manifest."""
    prefix = f"vercel-py-examples/{uuid4().hex}/collections/"
    report = prefix + "reports/2026/january.csv"
    archived_report = prefix + "archive/2026/january.csv"
    readme = prefix + "README.txt"
    csv_body = b"month,revenue\nJanuary,1200\n"
    readme_body = b"Monthly reports and their archived copies.\n"
    try:
        original = put_blob(report, csv_body, access=access, content_type="text/csv")
        put_blob(readme, readme_body, access=access, content_type="text/plain")
        copied = copy_blob(
            original.url,
            archived_report,
            access=access,
            content_type="text/csv",
            if_match=original.etag,
        )
        assert (blob.get_blob(copied.url, access=access)).body == csv_body

        # Flat listing includes all descendants. Pagination is automatic.
        print("All objects, including nested reports:")
        paths: set[str] = set()
        total_bytes = 0
        for item in query_blobs(prefix=prefix, page_size=2):
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
        for entry in query_entries(prefix=prefix, page_size=2):
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
        for item in walk_blobs(prefix):
            if item.pathname.endswith(".csv"):
                print(f"  {item.pathname.removeprefix(prefix)} -> {item.download_url}")
                csv_paths.add(item.pathname)
        assert csv_paths == {report, archived_report}
    finally:
        # Clean up known paths even if setup or traversal failed partway through.
        delete_blob([report, archived_report, readme])


def main() -> None:
    access: blob.Access = "private" if os.getenv("BLOB_TEST_ACCESS") == "private" else "public"
    pathname = f"vercel-py-examples/{uuid4().hex}/sync.bin"
    payload = bytes(range(256)) * 1024
    uploaded = blob.put_blob(
        pathname, payload, access=access, content_type="application/octet-stream"
    )
    try:
        result = blob.get_blob(uploaded.url, access=access)
        assert result.body == payload
        assert result.metadata.content_type == uploaded.content_type
        received = bytearray()
        with blob.stream_blob(uploaded.url, access=access) as download:
            for chunk in download:
                received.extend(chunk)
        assert received == payload
        metadata = blob.head_blob(uploaded.url)
        assert metadata.size == len(payload)
    finally:
        blob.delete_blob(uploaded.url)

    # Stream upload from a temporary file reader
    stream_pathname = f"vercel-py-examples/{uuid4().hex}/sync-stream.bin"
    with tempfile.NamedTemporaryFile("w+b") as tmp:
        tmp.write(payload)
        tmp.flush()
        tmp.seek(0)
        stream_uploaded = blob.put_blob(
            stream_pathname,
            tmp,
            access=access,
            content_length=len(payload),
            content_type="application/octet-stream",
        )
    try:
        stream_result = blob.get_blob(stream_uploaded.url, access=access)
        assert stream_result.body == payload
    finally:
        blob.delete_blob(stream_uploaded.url)

    # Parts stream independently; the final part may be smaller than 5 MiB.
    multipart_pathname = f"vercel-py-examples/{uuid4().hex}/sync-multipart.bin"
    with blob.multipart_upload_blob(multipart_pathname, access=access) as upload:
        upload.upload_part((b"m" * (64 * 1024) for _ in range(80)), content_length=5 * 1024 * 1024)
        upload.upload_part(b"tail")
    try:
        assert blob.get_blob(upload.result.url, access=access).body == (
            b"m" * (5 * 1024 * 1024) + b"tail"
        )
    finally:
        blob.delete_blob(upload.result.url)

    collections_example(access)

    print("Sync Blob lifecycle passed; test object deleted.")


if __name__ == "__main__":
    main()
