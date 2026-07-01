#!/usr/bin/env python3
"""Seek through a Blob and compare folded and expanded scans."""

from uuid import uuid4

from dotenv import load_dotenv

from vercel.unstable.blob import ScandirMode, sync as blob

load_dotenv()


def main() -> None:
    prefix = f"examples/unstable-blob/{uuid4().hex}/"
    pathname = f"{prefix}archives/large.bin"
    try:
        payload = bytes(range(256)) * 4096
        with blob.open(pathname, "wb", content_type="application/octet-stream") as target:
            target.write(payload)

        with blob.open(pathname, "rb") as source:
            source.seek(512 * 1024)
            print(f"middle: {source.read(16).hex()}")
            source.seek(-32, 1)
            print(f"backward: {source.read(16).hex()}")

        folded = list(blob.scandir(prefix))
        expanded = list(blob.scandir(prefix, mode=ScandirMode.EXPANDED))
        print("folded:", [entry.path for entry in folded])
        print("expanded:", [entry.path for entry in expanded])
    finally:
        blob.rmtree(prefix, missing_ok=True)


if __name__ == "__main__":
    main()
