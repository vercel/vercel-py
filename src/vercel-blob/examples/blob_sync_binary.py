"""Write, read, inspect, and remove a binary Blob synchronously."""

from __future__ import annotations

from uuid import uuid4

from vercel.api import session
from vercel.blob import sync as blob


def main() -> None:
    pathname = f"vercel-py-examples/{uuid4().hex}/payload.bin"
    with session():
        try:
            with blob.open(pathname, "wb") as writer:
                writer.write(b"vercel-blob\x00binary")

            metadata = blob.stat(pathname)
            with blob.open(pathname, "rb") as reader:
                print(f"{metadata.pathname} ({metadata.size} bytes): {reader.read()!r}")
        finally:
            blob.remove(pathname, missing_ok=True)


if __name__ == "__main__":
    main()
