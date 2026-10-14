"""Upload bytes, download buffered and streamed content, and delete the object."""

from __future__ import annotations

import os
from uuid import uuid4

import anyio

from vercel import blob


async def main() -> None:
    access: blob.Access = "private" if os.getenv("BLOB_TEST_ACCESS") == "private" else "public"
    pathname = f"vercel-py-examples/{uuid4().hex}/async.bin"
    payload = bytes(range(256)) * 1024
    uploaded = await blob.put(
        pathname, payload, access=access, content_type="application/octet-stream"
    )
    try:
        result = await blob.get(uploaded.url, access=access)
        assert result.body == payload
        assert result.metadata.content_type == uploaded.content_type
        received = bytearray()
        async with blob.stream(uploaded.url, access=access) as download:
            async for chunk in download:
                received.extend(chunk)
        assert received == payload
        metadata = await blob.head(uploaded.url)
        assert metadata.size == len(payload)
    finally:
        await blob.delete(uploaded.url)
    print("Async Blob lifecycle passed; test object deleted.")


if __name__ == "__main__":
    anyio.run(main)
