"""Stream a file from local storage through Blob and Sandbox and back."""

from __future__ import annotations

from collections.abc import Awaitable
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Protocol
from uuid import uuid4

import anyio

from vercel import blob, sandbox
from vercel.api import session

CHUNK_SIZE = 64 * 1024


class AsyncBinaryReader(Protocol):
    def read(self, size: int = -1) -> Awaitable[bytes]: ...


class AsyncBinaryWriter(Protocol):
    def write(self, data: bytes) -> Awaitable[int]: ...


async def copy_stream(source: AsyncBinaryReader, target: AsyncBinaryWriter) -> int:
    """Copy one asynchronous binary stream without buffering it all in memory."""
    copied = 0
    while chunk := await source.read(CHUNK_SIZE):
        copied += await target.write(chunk)
    return copied


async def main() -> None:
    prefix = f"vercel-py-examples/{uuid4().hex}"
    source_blob = f"{prefix}/source.txt"
    transformed_blob = f"{prefix}/uppercase.txt"
    payload = b"Hello from local storage, Blob, and Sandbox!\n" * 16_384

    async with session():
        try:
            with TemporaryDirectory() as directory:
                local_source = Path(directory) / "source.txt"
                local_result = Path(directory) / "uppercase.txt"
                local_source.write_bytes(payload)

                # Local -> Blob.
                async with (
                    await anyio.open_file(local_source, "rb") as source,
                    blob.open(source_blob, "wb", content_type="text/plain") as target,
                ):
                    await copy_stream(source, target)

                source_stat = await blob.stat(source_blob)
                async with sandbox.create_sandbox(execution_time_limit=timedelta(minutes=2)) as box:
                    # Blob -> Sandbox. Supplying the known size lets Sandbox stream
                    # directly instead of first spooling an unknown-size upload.
                    async with (
                        blob.open(source_blob, "rb") as source,
                        box.fs.open("workspace/source.txt", "wb", size=source_stat.size) as target,
                    ):
                        await copy_stream(source, target)

                    await box.run_process(
                        "python",
                        [
                            "-c",
                            (
                                "from pathlib import Path; import sys; "
                                "source, target = map(Path, sys.argv[1:]); "
                                "target.write_bytes(source.read_bytes().upper())"
                            ),
                            "workspace/source.txt",
                            "workspace/uppercase.txt",
                        ],
                        check=True,
                    )

                    # Sandbox -> Blob. Blob publishes the result when the writer closes.
                    async with (
                        box.fs.open("workspace/uppercase.txt", "rb") as source,
                        blob.open(transformed_blob, "wb", content_type="text/plain") as target,
                    ):
                        await copy_stream(source, target)

                # Blob -> local.
                async with (
                    blob.open(transformed_blob, "rb") as source,
                    await anyio.open_file(local_result, "wb") as target,
                ):
                    copied = await copy_stream(source, target)

                assert local_result.read_bytes() == payload.upper()
                print(f"Streamed and transformed {copied} bytes")
        finally:
            for pathname in (source_blob, transformed_blob):
                await blob.remove(pathname, missing_ok=True)


if __name__ == "__main__":
    anyio.run(main)
