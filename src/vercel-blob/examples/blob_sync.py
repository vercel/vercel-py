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

    print("Sync Blob lifecycle passed; test object deleted.")


if __name__ == "__main__":
    main()
