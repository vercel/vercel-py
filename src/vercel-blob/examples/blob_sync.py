"""Run the Blob object lifecycle without an event loop."""

from __future__ import annotations

import os
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
    print("Sync Blob lifecycle passed; test object deleted.")


if __name__ == "__main__":
    main()
