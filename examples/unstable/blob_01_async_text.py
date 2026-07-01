#!/usr/bin/env python3
"""Create and read a text object with the unstable async Blob API."""

from uuid import uuid4

import anyio
from dotenv import load_dotenv

from vercel.unstable import blob

load_dotenv()


async def main() -> None:
    prefix = f"examples/unstable-blob/{uuid4().hex}/"
    pathname = f"{prefix}greeting.txt"
    try:
        async with blob.open(pathname, "w", content_type="text/plain") as target:
            await target.write("Hello from vercel.unstable.blob!\n")

        async with blob.open(pathname, encoding="utf-8") as source:
            print(await source.read(), end="")

        info = await blob.stat(pathname)
        print(f"{info.pathname}: {info.size} bytes")
    finally:
        await blob.rmtree(prefix, missing_ok=True)


if __name__ == "__main__":
    anyio.run(main)
