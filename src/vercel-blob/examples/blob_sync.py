"""Run the Blob object lifecycle without an event loop."""

from __future__ import annotations

import os
import tempfile
from uuid import uuid4

from vercel.blob import sync as blob


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

    # Copy on the server, iterate over objects, then delete both objects.
    prefix = f"vercel-py-examples/{uuid4().hex}/collections/"
    source = prefix + "source.bin"
    destination = prefix + "nested/copy.bin"
    try:
        original = blob.put_blob(source, b"collection", access=access)
        copied = blob.copy_blob(
            original.url,
            destination,
            access=access,
            content_type="application/octet-stream",
            if_match=original.etag,
        )
        assert (blob.get_blob(copied.url, access=access)).body == b"collection"
        paths = {item.pathname for item in blob.query_blobs(prefix=prefix, page_size=1)}
        assert paths == {source, destination}
        entries = list(blob.query_entries(prefix=prefix, page_size=1))
        objects = {item.pathname for item in entries if isinstance(item, blob.ListBlob)}
        folders = [item for item in entries if isinstance(item, blob.BlobPrefix)]
        assert objects == {source}
        assert folders == [blob.BlobPrefix(prefix=prefix + "nested/")]
        nested = list(blob.query_entries(prefix=folders[0].prefix))
        assert len(nested) == 1
        assert isinstance(nested[0], blob.ListBlob)
        assert nested[0].pathname == destination
    finally:
        blob.delete_blob([source, destination])

    print("Sync Blob lifecycle passed; test object deleted.")


if __name__ == "__main__":
    main()
