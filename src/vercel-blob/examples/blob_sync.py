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
    uploaded = blob.put(pathname, payload, access=access, content_type="application/octet-stream")
    try:
        result = blob.get(uploaded.url, access=access)
        assert result.body == payload
        assert result.metadata.content_type == uploaded.content_type
        received = bytearray()
        with blob.stream(uploaded.url, access=access) as download:
            for chunk in download:
                received.extend(chunk)
        assert received == payload
        metadata = blob.head(uploaded.url)
        assert metadata.size == len(payload)
    finally:
        blob.delete(uploaded.url)

    # Stream upload from a temporary file reader
    stream_pathname = f"vercel-py-examples/{uuid4().hex}/sync-stream.bin"
    with tempfile.NamedTemporaryFile("w+b") as tmp:
        tmp.write(payload)
        tmp.flush()
        tmp.seek(0)
        stream_uploaded = blob.put(
            stream_pathname,
            tmp,
            access=access,
            content_length=len(payload),
            content_type="application/octet-stream",
        )
    try:
        stream_result = blob.get(stream_uploaded.url, access=access)
        assert stream_result.body == payload
    finally:
        blob.delete(stream_uploaded.url)

    print("Sync Blob lifecycle passed; test object deleted.")


if __name__ == "__main__":
    main()
