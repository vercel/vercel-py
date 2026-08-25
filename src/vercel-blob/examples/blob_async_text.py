"""Write, read, inspect, and remove a text Blob asynchronously."""

from __future__ import annotations

from uuid import uuid4

import anyio

from vercel import blob
from vercel.api import session


async def main() -> None:
    pathname = f"vercel-py-examples/{uuid4().hex}/message.txt"
    async with session():
        try:
            async with blob.open(pathname, "w", content_type="text/plain") as writer:
                await writer.write("Hello from vercel-blob.\n")

            metadata = await blob.stat(pathname)
            async with blob.open(pathname) as reader:
                print(f"{metadata.pathname} ({metadata.size} bytes): {await reader.read()}", end="")
        finally:
            await blob.remove(pathname, missing_ok=True)


if __name__ == "__main__":
    anyio.run(main)
